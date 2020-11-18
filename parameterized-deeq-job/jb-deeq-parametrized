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
  ApproxCountDistinct,
  Uniqueness,
  UniqueValueRatio,
  Sum,
  PatternMatch,
  MutualInformation,
  Minimum,
  Maximum,
  Entropy,
  Distinctness,
  DataType,
  CountDistinct
}



object GlueApp {
  def main(sysArgs: Array[String]) {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark = glueContext.getSparkSession
    val args =
      GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
// Read from input parameters

   val jobParam = GlueArgParser.getResolvedOptions(sysArgs, Array("JOB_NAME","s3_input_data_folder",
   "s3_output_data_folder","file_format","ddb_input_table","ddb_metric_result_table","ApproxCountDistinctCol",
   "CompletenessCol",	"ComplianceCol",	"ComplianceLogic",
   "CorrelationCol",	"CorrelationCol2",	"CountDistinctCol",	"DataTypeCol",	
   "DistinctnessCol",	"EntropyCol",	"MaximumCol",	"MeanCol",	"MinimumCol",
   "MutualInformationCol",	"MutualInformationOtheCol",		
   "SumCol",	"UniqueValueRatioCol",	"UniquenessCol"))
   
   val s3_input_data_folder = jobParam("s3_input_data_folder")
   val file_format = jobParam("file_format")
   val s3_output_data_folder = jobParam("s3_output_data_folder")
   val ddb_input_table = jobParam("ddb_input_table")
   val ddb_metric_result_table = jobParam("ddb_metric_result_table")
   val ApproxCountDistinctCol = jobParam("ApproxCountDistinctCol")
   
   // println ("quantileAsFloat : " + quantileAsFloat)
   val CompletenessCol = jobParam("CompletenessCol")
   val ComplianceCol = jobParam("ComplianceCol")
   val ComplianceLogic = jobParam("ComplianceLogic")
   val CorrelationCol = jobParam("CorrelationCol")
   val CorrelationCol2 = jobParam("CorrelationCol2")
   val CountDistinctCol = jobParam("CountDistinctCol")
   val DataTypeCol = jobParam("DataTypeCol")
   val DistinctnessCol = jobParam("DistinctnessCol")
   val EntropyCol = jobParam("EntropyCol")
   val MaximumCol = jobParam("MaximumCol")  
   
   val MeanCol = jobParam("MeanCol")
   val MinimumCol = jobParam("MinimumCol")
   val MutualInformationCol = jobParam("MutualInformationCol")
   val MutualInformationOtheCol = jobParam("MutualInformationOtheCol")
  
   val SumCol = jobParam("SumCol")
   val UniqueValueRatioCol = jobParam("UniqueValueRatioCol")
   val UniquenessCol = jobParam("UniquenessCol")
   


    val logger = LoggerFactory.getLogger(args("JOB_NAME"))
    import spark.implicits._ 
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.implicits._
    logger.info("Start Job")
    
// read the file names from s3 bucket and store in a dataframe

   val bucket = s3_input_data_folder // "vw-dpp-testdata-input"
   // val prefix = "input1"
   val s3Client = new AmazonS3Client()
   val req = new ListObjectsRequest().withBucketName(bucket)
   val objectListing = s3Client.listObjects(req) //s3Client.listObjects(req)
   val summaries = objectListing.getObjectSummaries().asScala;
   val filename = summaries.map(s3ObSummary => s3ObSummary.getKey())
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
        "dynamodb.output.tableName" -> ddb_input_table, // "dpp-input-file-names",
        "dynamodb.throughput.write.percent" -> "1.0"
      ))
    ).asInstanceOf[DynamoDbDataSink]
    
    dynamoDbSink.writeDynamicFrame(dynamicFrame)

    println("writing complete to input table of DDB " )
// End of writing to dynamodb input table
// start reading the bucket and file names from Dynamodb input tables to a dataframe 
    val dynamicFrameRead = glueContext.getSourceWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.input.tableName" -> ddb_input_table, // "dpp-input-file-names",
        "dynamodb.throughput.read.percent" -> "1.0",
        "dynamodb.splits" -> "100"
      ))
    ).getDynamicFrame()
    
    // print( dynamicFrameRead.getNumPartitions())
    
   val dfRead = dynamicFrameRead.toDF()
    
    println("df after read from DDB " + dfRead.show() )
    val dfReadNotProcessed = dfRead.filter(dfRead("processed") === "N")

   println("after applying filter for only NOT processed file " + dfReadNotProcessed.show() )
   
// For each of the NOT PROCESSED File run the analysis below to get metrics
   
   	for (row <- dfReadNotProcessed.rdd.collect)
{
        println("row :" + row)
    var bucketName = row.mkString(",").split(",")(1)
    var fileName = row.mkString(",").split(",")(2)
    
    var paths = "s3n://" + bucketName + "/" + fileName
    
    println("s3 input path  :" + paths)
    
    var dataset = spark.read
      .format(file_format)  //"csv")
      .option("header", "true")
      .load(
        paths
      )
      
      
    println("MeanCol  :" + MeanCol)   
     println("MaximumCol  :" + MaximumCol)   
      println("MinimumCol  :" + MinimumCol)   
      
   var analysisResult: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Size())
        .addAnalyzer(Completeness(CompletenessCol))  // "partnumber"))
        .addAnalyzer(ApproxCountDistinct(ApproxCountDistinctCol))  // "partnumber"))
        .addAnalyzer(Mean(MeanCol))  //"programnumber"))
        .addAnalyzer(Compliance(ComplianceCol, ComplianceLogic))    // "press", "press = 400"))
        .addAnalyzer(Correlation(CorrelationCol,CorrelationCol2))  // "incompletechecked", "failurefound"))
        .addAnalyzer(DataType(DataTypeCol))
	    .addAnalyzer(CountDistinct(CountDistinctCol))
		.addAnalyzer(Distinctness(DistinctnessCol))
		.addAnalyzer(Entropy(EntropyCol))
		.addAnalyzer(Maximum(MaximumCol))
		.addAnalyzer(Minimum(MinimumCol))
		.addAnalyzer(MutualInformation(  Seq(MutualInformationCol, MutualInformationOtheCol) ))
		//.addAnalyzer(PatternMatch(PatternMatchCol, pattern = PatternMatchPattern))
		.addAnalyzer(Sum(SumCol))
		.addAnalyzer(UniqueValueRatio(UniqueValueRatioCol))
		.addAnalyzer(Uniqueness(UniquenessCol))
        // compute metrics
        .run()
    }
    

     

// retrieve successfully computed metrics as a Spark data frame
    var metrics = successMetricsAsDataFrame(spark, analysisResult)
    
    println("metrics type ::" + metrics.getClass)
    
    println("metrics data ::" + metrics.show())
    
 
    
//Add file name, bucket name and the pk for each row of the metrics obtained and write the data to DynamoDB output table
    
    
    for (rowMetrics <- metrics.rdd.collect)
    {
        
    println("rowMetrics ::" + rowMetrics)
    
    println("rowMetrics type ::" + rowMetrics.getClass)
    
    //Extract the column values from the 1st row of the metrics and create a dataframe 
        
    var entity = rowMetrics.mkString(",").split(",")(0)
    var instance = rowMetrics.mkString(",").split(",")(1)
    var name = rowMetrics.mkString(",").split(",")(2)
    var value = rowMetrics.mkString(",").split(",")(3)
    println("name is ::" + name)
    println("entity is ::" + entity)
    println("instance is ::" + instance)
    println("value is ::" + value)

var rowMetricsToData = List(Row(entity , instance , name,value))

var schemaForMetric = StructType(List(
  StructField("entity", StringType, true),
  StructField("instance", StringType, true),
  StructField("name", StringType, true),
  StructField("value", StringType, true) ) )
  
   
     var rddrowMetricsToData = spark.sparkContext.parallelize(rowMetricsToData)
     var metricsDDB = spark.createDataFrame(rddrowMetricsToData, schemaForMetric)

    
    var bucket_file_entity_name = bucketName + "_" + fileName + "_" + entity + "_" + name
    
    println("bucket_file_entity_name :" + bucket_file_entity_name)
    
    //add bucket name, file name and a primary key (concatenation of bucket,file,entity and name ) to the dataframe created above
    
    metricsDDB = metricsDDB.withColumn("bucket_file_entity_name", lit(bucket_file_entity_name))
    metricsDDB = metricsDDB.withColumn("bucket-name", lit(bucketName))
    metricsDDB = metricsDDB.withColumn("file-name", lit(fileName))
    // println("after adding pk and other columns for DDB o/p : " + metricsDDB.show() )
    
   //writing metrics to dynamodb output table
   val dynamicFrameMetric = DynamicFrame(metricsDDB, glueContext)
    val dynamoDbSink: DynamoDbDataSink =  glueContext.getSinkWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.output.tableName" -> ddb_metric_result_table, // "dpp-output-metrics-result",
        "dynamodb.throughput.write.percent" -> "1.0"
      ))
    ).asInstanceOf[DynamoDbDataSink]
    
    dynamoDbSink.writeDynamicFrame(dynamicFrameMetric)

    println("writing complete the metrics to DDB " )
   
   // End of writing metric result to dynamodb output table
   
   // start of updating the file processed status in dynamodb after the file has been process and result is stored in dynamodb
   
   var dfToUpdateDDB = metricsDDB.select("bucket-name", "file-name")
    val alreadyProcessedfile = "Y"
    dfToUpdateDDB = dfToUpdateDDB.withColumn("processed", lit(alreadyProcessedfile))
    println("df after updating with Y : " + dfToUpdateDDB.show() )
    
    val dynamicFrameUpdateInput = DynamicFrame(dfToUpdateDDB, glueContext)
    
    val dynamoDbSinkToUpdateFileStatus: DynamoDbDataSink =  glueContext.getSinkWithFormat(
      connectionType = "dynamodb",
      options = JsonOptions(Map(
        "dynamodb.output.tableName" -> ddb_input_table, // "dpp-input-file-names",
        "dynamodb.throughput.write.percent" -> "1.0"
      ))
    ).asInstanceOf[DynamoDbDataSink]

    dynamoDbSinkToUpdateFileStatus.writeDynamicFrame(dynamicFrameUpdateInput)

    println("writing update complete to input table of DDB " )
   
    }

// Additinally write metrics result on s3 as well ( to be seen via Athena)

 var opS3Path = "s3n://" + s3_output_data_folder + "/" + fileName
 
 println("s3 output path " + opS3Path)
 
    metrics.write
      .option("header", "true")
      .mode("overwrite")
      .csv(opS3Path)
      
    println("s3 output write complete " )  
   


}


  }
}