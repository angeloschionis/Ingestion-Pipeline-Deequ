import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{S3ObjectSummary, ObjectListing, GetObjectRequest,ListObjectsRequest }
import scala.collection.JavaConversions.{collectionAsScalaIterable => asScala}
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col,lower, trim, when, lit}
import org.slf4j.LoggerFactory

import com.amazonaws.services.glue.DynamoDbDataSink
import com.amazonaws.services.glue.DynamicFrame

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{
  Compliance,
  Correlation,
  Size,
  Completeness,
  Mean,
  ApproxCountDistinct
}


object GlueApp {
  def main(sysArgs: Array[String]) {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark = glueContext.getSparkSession
    val args =
      GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val logger = LoggerFactory.getLogger(args("JOB_NAME"))
    import spark.implicits._ 
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.implicits._
    logger.info("Start Job")
    
// read the file names from s3 bucket and store in a dataframe

   val bucket = "achionis-deequ-testdata-input"
   // val prefix = "input1" // In case prefix is required
   val s3Client = new AmazonS3Client()
   val req = new ListObjectsRequest().withBucketName(bucket)
   val objectListing = s3Client.listObjects(req) //s3Client.listObjects(req)
   val summaries = objectListing.getObjectSummaries().asScala;
   val filename = summaries.map(s3ObSummary => s3ObSummary.getKey())

   // the schema of the df is defined
   val schema = StructType(List(
  StructField("bucket-name", StringType, true),
  StructField("file-name", StringType, true),
  StructField("processed", StringType, true)))
   var df = spark.createDataFrame(sc.emptyRDD[Row], schema)
   val processedfile = "N"
   for (x <- filename) {
     var  csvFileName = x
     var dataRow1 = List(Row(bucket , csvFileName , processedfile))
     var rddDataRow = spark.sparkContext.parallelize(dataRow1)
     var dfFile = spark.createDataFrame(rddDataRow, schema)
     df = df.unionAll(dfFile)
    }
    println("df values " + df.show() ) 
    
// end of reading file names from s3

//writing the file names to dynamodb input table

    val dynamicFrame = DynamicFrame(df, glueContext)
    val dynamoDbSink: DynamoDbDataSink =  glueContext.getSinkWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.output.tableName" -> "dpp-input-file-names",
        "dynamodb.throughput.write.percent" -> "1.0"
      ))
    ).asInstanceOf[DynamoDbDataSink]
    
    dynamoDbSink.writeDynamicFrame(dynamicFrame)

    println("writing complete to input table of DDB " )
// End of writing to dynamodb

// start reading the bucket and file names from Dynamodb input tables to a dataframe 
    val dynamicFrameRead = glueContext.getSourceWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.input.tableName" -> "dpp-input-file-names",
        "dynamodb.throughput.read.percent" -> "1.0",
        "dynamodb.splits" -> "100"
      ))
    ).getDynamicFrame()
    
    // print( dynamicFrameRead.getNumPartitions())
    
   val dfRead = dynamicFrameRead.toDF()
    
    println("df after read from DDB " + dfRead.show() )

    //filter to consider only those files whose processing status in N
    val dfReadNotProcessed = dfRead.filter(dfRead("processed") === "N")

    println("after applying filter " + dfReadNotProcessed.show() )
   
   val schemaBucketAndFile = StructType(List(
  StructField("bucket-name", StringType, true),
  StructField("file-name", StringType, true)))

  // For each of the NOT PROCESSED File run the analysis below to get metrics
   
    for (row <- dfReadNotProcessed.rdd.collect)
{
        println("row :" + row)
    var bucketName = row.mkString(",").split(",")(1)
    var fileName = row.mkString(",").split(",")(2)
    
    var paths = "s3n://" + bucketName + "/" + fileName
    
    println("s3 input path  :" + paths)
    
    var dataset = spark.read
      .format("csv")
      .option("header", "true")
      .load(
        paths
      )
    var analysisResult: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("partnumber"))
        .addAnalyzer(ApproxCountDistinct("partnumber"))
        .addAnalyzer(Mean("programnumber"))
        .addAnalyzer(Compliance("press", "press = 400"))
        .addAnalyzer(Correlation("incompletechecked", "failurefound"))
        .addAnalyzer(Correlation("presscounter", "failurefound"))
        // compute metrics
        .run()
    }

// retrieve successfully computed metrics as a Spark data frame
    var metrics = successMetricsAsDataFrame(spark, analysisResult)
    
    println("raw metric : " + metrics.show())
    
    for (row1 <- metrics.rdd.collect)
    {
        
    var entity = row1.mkString(",").split(",")(0)
    var name = row1.mkString(",").split(",")(2)
    
    var bucket_file_entity_name = bucketName + "_" + fileName + "_" + entity + "_" + name
    
    println("bucket_file_entity_name :" + bucket_file_entity_name)
    
    var metricsDDB = metrics.withColumn("bucket_file_entity_name", lit(bucket_file_entity_name))
    metricsDDB = metricsDDB.withColumn("bucket-name", lit(bucketName))
    metricsDDB = metricsDDB.withColumn("file-name", lit(fileName))
   //println("after adding pk for DDB : " + metricsDDB.show() )
    
   //writing metrics to dynamodb
   val dynamicFrameMetric = DynamicFrame(metricsDDB, glueContext)
    val dynamoDbSink: DynamoDbDataSink =  glueContext.getSinkWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.output.tableName" -> "dpp-output-metrics-result",
        "dynamodb.throughput.write.percent" -> "1.0"
      ))
    ).asInstanceOf[DynamoDbDataSink]
    
    dynamoDbSink.writeDynamicFrame(dynamicFrameMetric)

    println("writing complete the metrics to DDB " )
   
   // End of writing metric result to dynamodb
   
   // start of updating the file processed status in dynamodb after the file has been process and result is stored in dynamodb
   
   var dfToUpdateDDB = metricsDDB.select("bucket-name", "file-name")
    val alreadyProcessedfile = "Y"
    dfToUpdateDDB = dfToUpdateDDB.withColumn("processed", lit(alreadyProcessedfile))
    println("df after updating with Y : " + dfToUpdateDDB.show() )
    
    val dynamicFrameUpdateInput = DynamicFrame(dfToUpdateDDB, glueContext)
    
    val dynamoDbSinkToUpdateFileStatus: DynamoDbDataSink =  glueContext.getSinkWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.output.tableName" -> "dpp-input-file-names",
        "dynamodb.throughput.write.percent" -> "1.0"
      ))
    ).asInstanceOf[DynamoDbDataSink]

    dynamoDbSinkToUpdateFileStatus.writeDynamicFrame(dynamicFrameUpdateInput)

    println("writing update complete to input table of DDB " )
   
    }

// Additinally write metrics result on s3 as well ( to be seen via Athena)

 var opS3Path = "s3n://" + "achionis-deequ-testdata-output" + "/" + fileName
 
 println("s3 output path " + opS3Path)
 
    metrics.write
      .option("header", "true")
      .mode("overwrite")
      .csv(opS3Path)
      
    println("s3 output write complete " )

   }
  }
}