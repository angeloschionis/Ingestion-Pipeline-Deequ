# Ingestion-Pipeline-Deequ

*Pre-requisite*
Before creation of the Glue job (mentioned below) , please create 2 DynamoDB db tables. The idea is after the new files arrive to S3 bucket, the glue job needs to extract the file names and store in a DynamoDB input table (*dpp-input-file-names*)with processing status = N. After the job processed the files the metric result will be stored to another DynamoDb output table table (*dpp-output-metrics-result*)  and it will update the file processed status as Y to input table. 
The details of keys and sample screen print of these tables are mentioned below.

a) Input table - “dpp-input-file-names”  to store the bucket name, file names and processed status. Before processing the file the processed status would be = N. After processing the file, the status would be updated to Y.

b) Output table - “dpp-output-metrics-result” to store the metric result for each of the file. Here the primary key has been created by combination of the business keys to make it unique - bucketName  and FileName and entity and name of the attribute that we are are measuring.

## Glue Job creation

Create the Glue job with the script SuggestionRunner
important parameters :
1. use Spark 2.4 , Scala 2 (glue 1.0)
2. provide the dependant jars (deequ-1.0.1.jar) in the S3 path as - s3://vw-deeq-jar-scripts  --> Here you may choose your bucket name as well.
3. Enable Glue Catalog for Hive Metastore
4. The role chosen while creation of Glue job should have access to S3 and DynamoDB

[Image: image.png]


