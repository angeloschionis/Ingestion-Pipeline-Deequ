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

//package com.amazon.deequ.examples

//import ExampleUtils.{manufacturersAsDataframe, withSpark}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.analyzers.{Analysis, InMemoryStateProvider}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
//import com.amazon.deequ.checks.{Check, CheckLevel}

import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.examples.ExampleUtils
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.google.common.io.Files

import java.io.File

//import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
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
    StructField("bucket_name", StringType, true),
    StructField("file_name", StringType, true),
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
   
   for (row <- dfReadNotProcessed.rdd.collect) {
       println("row :" + row)
       var bucketName = row.mkString(",").split(",")(2)
       var fileName = row.mkString(",").split(",")(1)
    
       var paths = "s3://" + bucketName + "/" + fileName
    
       println("s3 input path  :" + paths)
    
       var dataset = spark.read
         .format(file_format)  //"csv")
         .option("header", "true")
         .load(
           paths
         )

// Start of the metrics calculation based on the job parameter.   
// Calculate metrics Size()   
 
        val analysisResult = Analysis()
          .addAnalyzer(Size())
  
  
        var stateStore = InMemoryStateProvider()

        val metricsForDataSize = AnalysisRunner.run(
          data = dataset,
          analysis = analysisResult,
          saveStatesWith = Some(stateStore))
  
        println("Metrics for the size of the data:\n")
        metricsForDataSize.metricMap.foreach { case (analyzer, metric) =>
        println(s"\t$analyzer: ${metric.value.get}")
  
        val metricsSize = successMetricsAsDataFrame(spark, metricsForDataSize)
        println("metricsSize data ::" + metricsSize.show())
  
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

              val analysisResultC = Analysis()
             .addAnalyzer(Completeness(e))
  
             var stateStore = InMemoryStateProvider()

             val metricsForDataCompleteness = AnalysisRunner.run(
             data = dataset,
             analysis = analysisResultC,
             saveStatesWith = Some(stateStore))
  
             val metricsComplt = successMetricsAsDataFrame(spark, metricsForDataCompleteness)

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
    

             val analysisResultM = Analysis()
            .addAnalyzer(Mean(e))
  
             var stateStore = InMemoryStateProvider()

             val metricsForDataMean = AnalysisRunner.run(
             data = dataset,
             analysis = analysisResultM,
             saveStatesWith = Some(stateStore))
  
             val metricsMe = successMetricsAsDataFrame(spark, metricsForDataMean)

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

               val analysisResultA = Analysis()
              .addAnalyzer(ApproxCountDistinct(e))
  
               var stateStore = InMemoryStateProvider()

               val metricsForApproxCount = AnalysisRunner.run(
               data = dataset,
               analysis = analysisResultA,
               saveStatesWith = Some(stateStore))
    
               val metricsApproxCnt = successMetricsAsDataFrame(spark, metricsForApproxCount)
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
    
              val analysisResultCountDist = Analysis()
              .addAnalyzer(CountDistinct(e))
  
              var stateStore = InMemoryStateProvider()

              val metricsForCountDist = AnalysisRunner.run(
              data = dataset,
              analysis = analysisResultCountDist,
              saveStatesWith = Some(stateStore))
    
              val metricsCountDist = successMetricsAsDataFrame(spark, metricsForCountDist)
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
	           
               val analysisResultDataType = Analysis()
               .addAnalyzer(DataType(e))
  
               var stateStore = InMemoryStateProvider()

               val metricsForDataType = AnalysisRunner.run(
               data = dataset,
               analysis = analysisResultDataType,
               saveStatesWith = Some(stateStore))
    
               val metricsDataT = successMetricsAsDataFrame(spark, metricsForDataType)
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

                 val analysisResultDistinctN = Analysis()
                .addAnalyzer(Distinctness(e))
  
                 var stateStore = InMemoryStateProvider()

                 val metricsForDistinctN = AnalysisRunner.run(
                 data = dataset,
                 analysis = analysisResultDistinctN,
                 saveStatesWith = Some(stateStore))
	
                 val metricsDistN = successMetricsAsDataFrame(spark, metricsForDistinctN)
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
    
                 val analysisResultDistinctEntropy = Analysis()
                 .addAnalyzer(Entropy(e))
  
                  var stateStore = InMemoryStateProvider()

                  val metricsForDistinctEntropy = AnalysisRunner.run(
                  data = dataset,
                  analysis = analysisResultDistinctEntropy,
                  saveStatesWith = Some(stateStore))
    
                  val metricsEn = successMetricsAsDataFrame(spark, metricsForDistinctEntropy)
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
	
                val analysisResultDistinctMaximum = Analysis()
                .addAnalyzer(Maximum(e))
  
                var stateStore = InMemoryStateProvider()

                val metricsForDistinctMaximum = AnalysisRunner.run(
                data = dataset,
                analysis = analysisResultDistinctMaximum,
                saveStatesWith = Some(stateStore))
    
                val metricsMx = successMetricsAsDataFrame(spark, metricsForDistinctMaximum)
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

               val analysisResultDistinctMin = Analysis()
              .addAnalyzer(Minimum(e))
  
               var stateStore = InMemoryStateProvider()

               val metricsForDistinctMin = AnalysisRunner.run(
               data = dataset,
               analysis = analysisResultDistinctMin,
               saveStatesWith = Some(stateStore))
    
               val metricsMin = successMetricsAsDataFrame(spark, metricsForDistinctMin)
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

                val analysisResultSum = Analysis()
                .addAnalyzer(Sum(e ))
  
                var stateStore = InMemoryStateProvider()

                val metricsForSum = AnalysisRunner.run(
                data = dataset,
                analysis = analysisResultSum,
                saveStatesWith = Some(stateStore))

    
                val metricsSm= successMetricsAsDataFrame(spark, metricsForSum)
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

                val analysisUniqR = Analysis()
               .addAnalyzer(UniqueValueRatio(e ))
  
                var stateStore = InMemoryStateProvider()

                val metricsForUniqR = AnalysisRunner.run(
                data = dataset,
                analysis = analysisUniqR,
                saveStatesWith = Some(stateStore))

   
                val metricsUniqR= successMetricsAsDataFrame(spark, metricsForUniqR)
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
	
                val analysisUniqN = Analysis()
               .addAnalyzer(Uniqueness(e ))
  
                var stateStore = InMemoryStateProvider()

                val metricsForUniqN = AnalysisRunner.run(
                data = dataset,
                analysis = analysisUniqN,
                saveStatesWith = Some(stateStore))
    
                val metricsUniqN= successMetricsAsDataFrame(spark, metricsForUniqN)
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
	
                val analysisResultCompl = Analysis()
               .addAnalyzer(Compliance(ComplianceColEach(e),ComplianceLogicEach(e)))
  
                var stateStore = InMemoryStateProvider()

                val metricsForCompliance = AnalysisRunner.run(
                data = dataset,
                analysis = analysisResultCompl,
                saveStatesWith = Some(stateStore))
    
                val metricsCompl = successMetricsAsDataFrame(spark, metricsForCompliance)
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

                 val analysisResultCorr = Analysis()
                .addAnalyzer(Correlation(CorrelationColEach(e),CorrelationCol2Each(e)))
  
                 var stateStore = InMemoryStateProvider()

                 val metricsForCorr = AnalysisRunner.run(
                 data = dataset,
                 analysis = analysisResultCorr,
                 saveStatesWith = Some(stateStore))
    
                  val metricsCorr = successMetricsAsDataFrame(spark, metricsForCorr)
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
	
                val analysisResultMutualI = Analysis()
               .addAnalyzer(MutualInformation(Seq(MutualInformationColEach(e), MutualInformationOtheColEach(e)) ))
  
                var stateStore = InMemoryStateProvider()

                val metricsForMutualI = AnalysisRunner.run(
                data = dataset,
                analysis = analysisResultMutualI,
                saveStatesWith = Some(stateStore))
    
                val metricsMutualI = successMetricsAsDataFrame(spark, metricsForMutualI)
                println("metricsMutualI data ::" + metricsMutualI.show())
                metricsMutualInformation= metricsMutualInformation.unionAll(metricsMutualI)
	   }
	}


// Create the final dataframe combining all the metrics calculated separately
	 
    var metricsFinal = metricsSize.unionAll(metricsCompleteness).unionAll(metricsApproxCountDistinct).unionAll(metricsMean).unionAll(metricsCompliance).unionAll(metricsCorrelation).unionAll(metricsCountDistinct).unionAll(metricsDataType).unionAll(metricsDistinctness).unionAll(metricsEntropy).unionAll(metricsMaximum).unionAll(metricsMinimum).unionAll(metricsMutualInformation).unionAll(metricsSum).unionAll(metricsUniqueValueRatio).unionAll(metricsUniqueness)
     println("metricsFinal data ::" + metricsFinal.show())
     
    
//Add file name, bucket name and the pk for each row of the metrics obtained and write the data to DynamoDB output table
    
    for (rowMetrics <- metricsFinal.rdd.collect){

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
    
         metricsDDB = metricsDDB.withColumn("bucket_file_name", lit(bucket_file_name))
         metricsDDB = metricsDDB.withColumn("col_metricName", lit(col_metricName))
         metricsDDB = metricsDDB.withColumn("bucket_name", lit(bucketName))
         metricsDDB = metricsDDB.withColumn("file_name", lit(fileName))
         println("after adding pk and other columns for DDB o/p : " + metricsDDB.show() )
    
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
   
         var dfToUpdateDDB = metricsDDB.select("bucket_name", "file_name")
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
   
// Additinally write metricsFinal result on s3 as well ( to be seen via Athena)

    var opS3Path = "s3://" + s3_output_data_folder + "/" + fileName
 
    println("s3 output path " + opS3Path)
 
    metricsFinal.write
      .option("header", "true")
      .mode("overwrite")
      .csv(opS3Path)
      
    println("s3 output write complete " )  
    
}


}


  }
}