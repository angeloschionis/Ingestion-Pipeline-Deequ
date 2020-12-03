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
      
  // Start of the metrics calculation based on the job parameter.   
     // Calculate metrics Size()   
   var analysisResult: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Size())
        .run()
    }
    
   val metrics = successMetricsAsDataFrame(spark, analysisResult)
   
   
// Start of computation of other metrics 

 // define and create an empty dataframe for each metrics.
 
  val metricsSchema = StructType(List(
  StructField("entity", StringType, true),
  StructField("instance", StringType, true),
  StructField("name", StringType, true),
  StructField("value", StringType, true)))
   var metricsCompleteness = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsApproxCountDistinct = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsMean = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsCompliance = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsCorrelation = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsCountDistinct = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsDataType = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsDistinctness = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsEntropy = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsMaximum = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsMinimum = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsMutualInformation = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsSum = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsUniqueValueRatio = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   var metricsUniqueness = spark.createDataFrame(sc.emptyRDD[Row], metricsSchema)
   
  // Start measuring metrics based on the input parameters. 
  // In case multiple columns are provided to calculate a metrics, split the columns and run the loop accordingly
  
  //Calculate Completeness Matrics
    
if (CompletenessCol != null || CompletenessCol != ""){	    

    val CompletenessColEach = CompletenessCol.split(",")

	   println("after splitting to a variable anfd foreach :" ) 
	   CompletenessColEach.foreach(println)
	   
	   for (e <- CompletenessColEach) {
    
  var analysisResultC: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Completeness(e))
        // compute metrics
        .run()
    }
    
    val metricsComplt = successMetricsAsDataFrame(spark, analysisResultC)
    println("metricsC data ::" + metricsComplt.show())
    metricsCompleteness = metricsCompleteness.unionAll(metricsComplt)
	   }
	}
	
//Calculate Mean Matrics

	if (MeanCol != null || MeanCol != ""){

    val MeanColEach = MeanCol.split(",")

	   println("after splitting to a variable anfd foreach :" ) 
	   MeanColEach.foreach(println)
	   
	   for (e <- MeanColEach) {
    
var analysisResultM: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Mean(e))
        // compute metrics
        .run()
    }
    
    val metricsMe = successMetricsAsDataFrame(spark, analysisResultM)
    println("metricsMe data ::" + metricsMe.show())
    metricsMean= metricsMean.unionAll(metricsMe)
	   }
	}
	

//Calculate ApproxCountDistinct Matrics
if (ApproxCountDistinctCol != null || ApproxCountDistinctCol != ""){
	    

    val ApproxCountDistinctColEach = ApproxCountDistinctCol.split(",")

	   println("after splitting to a variable anfd foreach :" ) 
	   ApproxCountDistinctColEach.foreach(println)
	   
	   for (e <- ApproxCountDistinctColEach) {
    
var analysisResultA: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(ApproxCountDistinct(e))
        // compute metrics
        .run()
    }
    
    val metricsApproxCnt = successMetricsAsDataFrame(spark, analysisResultA)
    println("metricsApproxCnt data ::" + metricsApproxCnt.show())
    metricsApproxCountDistinct = metricsApproxCountDistinct.unionAll(metricsApproxCnt)
	   }
	}
	
//Calculate CountDistinct Matrics	
	
	if (CountDistinctCol != null || CountDistinctCol != ""){

    val CountDistinctColEach = CountDistinctCol.split(",")

	   println("after splitting to a variable anfd foreach :" ) 
	   CountDistinctColEach.foreach(println)
	   
	   for (e <- CountDistinctColEach) {
    
var analysisResultCountDist: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(CountDistinct(e))
        // compute metrics
        .run()
    }
    
    val metricsCountDist = successMetricsAsDataFrame(spark, analysisResultCountDist)
    println("metricsCountDist data ::" + metricsCountDist.show())
    metricsCountDistinct= metricsCountDistinct.unionAll(metricsCountDist)
	   }
	}
	
//Calculate DataType Matrics	

	if (DataTypeCol != null || DataTypeCol != ""){
    val DataTypeColEach = DataTypeCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   DataTypeColEach.foreach(println)
	   
	   for (e <- DataTypeColEach) {
    
var analysisResultDataType: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(DataType(e))
        // compute metrics
        .run()
    }
    
    val metricsDataT = successMetricsAsDataFrame(spark, analysisResultDataType)
    println("metricsDataT data ::" + metricsDataT.show())
    metricsDataType= metricsDataType.unionAll(metricsDataT)
	   }
	}
	
//Calculate Distinctness Matrics

	if (DistinctnessCol != null || DistinctnessCol != ""){
    val DistinctnessColEach = DistinctnessCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   DistinctnessColEach.foreach(println)
	   
	   for (e <- DistinctnessColEach) {
    
var analysisResultDistinctN: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Distinctness(e))
        // compute metrics
        .run()
    }
    
    val metricsDistN = successMetricsAsDataFrame(spark, analysisResultDistinctN)
    println("metricsDistN data ::" + metricsDistN.show())
    metricsDistinctness= metricsDistinctness.unionAll(metricsDistN)
	   }
	}
//Calculate Entropy Matrics

	if (EntropyCol != null || EntropyCol != ""){
	    
    val EntropyColEach = EntropyCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   EntropyColEach.foreach(println)
	   
	   for (e <- EntropyColEach) {
    
var analysisResultDistinctEntropy: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Entropy(e))
        // compute metrics
        .run()
    }
    
    val metricsEn = successMetricsAsDataFrame(spark, analysisResultDistinctEntropy)
    println("metricsEn data ::" + metricsEn.show())
    metricsEntropy= metricsEntropy.unionAll(metricsEn)
	   }
	}
	

//Calculate Maximum Matrics

	if (MaximumCol != null || MaximumCol != ""){

    val MaximumColEach = MaximumCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   MaximumColEach.foreach(println)
	   
	   for (e <- MaximumColEach) {
    
var analysisResultDistinctMaximum: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Maximum(e))
        // compute metrics
        .run()
    }
    
    val metricsMx = successMetricsAsDataFrame(spark, analysisResultDistinctMaximum)
    println("metricsMx data ::" + metricsMx.show())
    metricsMaximum= metricsMaximum.unionAll(metricsMx)
	   }
	}
	
//Calculate Minimum Matrics

	if (MinimumCol != null || MinimumCol != ""){

    val MinimumColEach = MinimumCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   MinimumColEach.foreach(println)
	   
	   for (e <- MinimumColEach) {
    
var analysisResultDistinctMin: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Minimum(e))
        // compute metrics
        .run()
    }
    
    val metricsMin = successMetricsAsDataFrame(spark, analysisResultDistinctMin)
    println("metricsMin data ::" + metricsMin.show())
    metricsMinimum= metricsMinimum.unionAll(metricsMin)
	   }
	}
	
//Calculate Sum Matrics

	if (SumCol != null || SumCol != ""){

    val SumColEach = SumCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   SumColEach.foreach(println)
	   
	   for (e <- SumColEach) {
    
  var analysisResultSum: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Sum(e ))
        // compute metrics
        .run()
    }
    
    val metricsSm= successMetricsAsDataFrame(spark, analysisResultSum)
    println("metricsSm data ::" + metricsSm.show())
    metricsSum= metricsSum.unionAll(metricsSm)
	   }
	}
	

//Calculate UniqueValueRatio Matrics

	if (UniqueValueRatioCol != null || UniqueValueRatioCol != ""){

    val UniqueValueRatioColEach = UniqueValueRatioCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   UniqueValueRatioColEach.foreach(println)
	   
	   for (e <- UniqueValueRatioColEach) {
    
 var analysisUniqR: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(UniqueValueRatio(e ))
        // compute metrics
        .run()
    }
    
    val metricsUniqR= successMetricsAsDataFrame(spark, analysisUniqR)
    println("metricsUniqR data ::" + metricsUniqR.show())
    metricsUniqueValueRatio= metricsUniqueValueRatio.unionAll(metricsUniqR)
	   }
	}
	
//Calculate Uniqueness Matrics

	if (UniquenessCol != null || UniquenessCol != ""){

    val UniquenessColEach = UniquenessCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   UniquenessColEach.foreach(println)
	   
	   for (e <- UniquenessColEach) {
    
var analysisUniqN: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Uniqueness(e ))
        // compute metrics
        .run()
    }
    
    val metricsUniqN= successMetricsAsDataFrame(spark, analysisUniqN)
    println("metricsUniqN data ::" + metricsUniqN.show())
    metricsUniqueness= metricsUniqueness.unionAll(metricsUniqN)
	   }
	}
	
//Calculate Compliance Matrics	
	
	if (ComplianceCol != null || ComplianceCol != ""){

    val ComplianceColEach = ComplianceCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   ComplianceColEach.foreach(println)
	   
    val ComplianceLogicEach = ComplianceLogic.split(",")
	println("after splitting to a variable and foreach :" ) 
	   ComplianceLogicEach.foreach(println)
	   
	   
	   for (e <- ComplianceColEach.indices) {
    
var analysisResultCompl: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Compliance(ComplianceColEach(e),ComplianceLogicEach(e)))
        // compute metrics
        .run()
    }
    
    val metricsCompl = successMetricsAsDataFrame(spark, analysisResultCompl)
    println("metricsCompl data ::" + metricsCompl.show())
    metricsCompliance= metricsCompliance.unionAll(metricsCompl)
	   }
	}
	
//Calculate Correlation Matrics

	if (CorrelationCol != null || CorrelationCol != ""){
    val CorrelationColEach = CorrelationCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   CorrelationColEach.foreach(println)
	   
    val CorrelationCol2Each = CorrelationCol2.split(",")
	println("after splitting to a variable and foreach :" ) 
	   CorrelationCol2Each.foreach(println)   
	   
	   
	   for (e <- CorrelationColEach.indices) {

var analysisResultCorr: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(Correlation(CorrelationColEach(e),CorrelationCol2Each(e)))
        // compute metrics
        .run()
    }
    
    val metricsCorr = successMetricsAsDataFrame(spark, analysisResultCorr)
    println("metricsCorr data ::" + metricsCorr.show())
    metricsCorrelation= metricsCorrelation.unionAll(metricsCorr)
	   }
	}
	

//Calculate MutualInformatio Matrics

	if (MutualInformationCol != null || MutualInformationCol != ""){

    val MutualInformationColEach = MutualInformationCol.split(",")

	   println("after splitting to a variable and foreach :" ) 
	   MutualInformationColEach.foreach(println)
	   
    val MutualInformationOtheColEach = MutualInformationOtheCol.split(",")
	println("after splitting to a variable and foreach :" ) 
	   MutualInformationOtheColEach.foreach(println)   
	   
	   
	   for (e <- MutualInformationColEach.indices) {
    
var analysisResultMutualI: AnalyzerContext = {
      AnalysisRunner
      // data to run the analysis on
        .onData(dataset)
        // define analyzers that compute metrics
        .addAnalyzer(MutualInformation(Seq(MutualInformationColEach(e), MutualInformationOtheColEach(e)) ))
        // compute metrics
        .run()
    }
    
    val metricsMutualI = successMetricsAsDataFrame(spark, analysisResultMutualI)
    println("metricsMutualI data ::" + metricsMutualI.show())
    metricsMutualInformation= metricsMutualInformation.unionAll(metricsMutualI)
	   }
	}
	
     
 // Create the final dataframe combining all the metrics calculated separately
	 
    var metricsFinal = metrics.unionAll(metricsCompleteness).unionAll(metricsApproxCountDistinct).unionAll(metricsMean).unionAll(metricsCompliance).unionAll(metricsCorrelation).unionAll(metricsCountDistinct).unionAll(metricsDataType).unionAll(metricsDistinctness).unionAll(metricsEntropy).unionAll(metricsMaximum).unionAll(metricsMinimum).unionAll(metricsMutualInformation).unionAll(metricsSum).unionAll(metricsUniqueValueRatio).unionAll(metricsUniqueness)
     println("metricsFinal data ::" + metricsFinal.show())
     
    
//Add file name, bucket name and the pk for each row of the metrics obtained and write the data to DynamoDB output table
    
    
    for (rowMetrics <- metricsFinal.rdd.collect)
    {
        

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

   
   var bucket_file_name = bucketName + "-" + fileName
   println("bucket_file_name :" + bucket_file_name)
   var col_metricName = instance + "-" + name
   println("col_metricName :" + col_metricName)
    

    //add bucket name, file name and a primary key (concatenation of bucket,file,entity and name ) to the dataframe created above
    
    metricsDDB = metricsDDB.withColumn("bucket-file-name", lit(bucket_file_name))
    metricsDDB = metricsDDB.withColumn("col-metricName", lit(col_metricName))
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