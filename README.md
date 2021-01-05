Data Quality as part of a Data Catalogue



As data grows it is very important for modern organisations to understand what data exists and the data quality in their data stores (such as S3). With this artefact we will demonstrate how you can discover what data exists in your S3 buckets and the data quality in an automated way. This enables the creation of data lakes with trusted data without having to manually check what exists and the integrity of your data.

Solution Architecture
The solution is based on Amazon’s Deequ ( https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/ ). Deequ is implemented on top fo Apache Spark and is designed to scale with large datasets that typically live in a distributed filesystem or a data warehouse. Deequ works on tabular data, e.g., CSV files, database tables, logs, flattened json files, basically anything that you can fit into a Spark dataframe.
This solution is based on the following approach:
1)	Scan a given bucket and get the names of available files and store it to a DynamoDB table
2)	A Glue job that grabs the values from step one and runs predefined data quality checks against the available files.
3)	Results are stored to a DynamoDB table so they can be consumed by a data catalogue or any reporting tool
4) 	Results are also stored on S3 as we will utilize them on later releases as part of lineage report
5) 	Results are displayed into a central portal where users can also define the data quality metrics.

![1](https://user-images.githubusercontent.com/61417780/94172152-70d66300-fe92-11ea-8307-b221e4a0c824.jpg) 
 
All the infrastructure is managed through AWS CloudFormation as well the parameters and the setup of the tool to work on our data.

How to use the DQ Tool
Step 1:
Download the necessary files for our tool from this Github repository to your local PC or S3 bucket:
https://github.com/angeloschionis/Ingestion-Pipeline-Deequ 
-	DeeQuAnalysis-Parameterized.scala – Scala code that contains all the logic of our DQ Tool
-	glue-job-deeq-cfn-new.yaml – CloudFormation Script
-	deequ-1.0.1.jar – Jar that we need to run DeeQu library 


Step 2:
Upload the files DeeQuAnalysis-Parameterized.scala and deequ-1.0.1.jar in a S3 bucket. Make sure that the bucket you will be using is at the same region as the data you will be scanning with the DQ Tool.

Step 3: 
Now we need to create our AWS Cloud Formation Template.

Navigate to AWS CloudFormation and select  Create Stack  With New Resources (standard) and then select  Template is ready and   Amazon S3 URL / Upload a template file based on where you have stored glue-job-deeq-cfn-new.yaml.
 

 

Step 4:
Click next and fill all the necessary information as per the description of each field.
Tips:
-	Make sure that all your buckets are in the same region
-	Make sure that the AWS CloudFormation stack is created at the same region as your buckets
-	For parameters “S3BucketForDeequJar” and “S3BucketForScript” use the full S3 bucket URL for the object i.e. s3://my-bucket-name/deequ-1.0.1.jar
-	For Parameters “S3BucketForInputData” and “S3BucketForOutputData” just give the S3 bucket name which is unique worldwide i.e. my-unique-bucket-name
-	For parameter “tempDir” you can use one of your input or output buckets or you can just use a new bucket
-	For “DynamoDB Table names” you can leave the parameters empty and the CloudFormation script will create DynamoDB tables for you. If you wish to use pre-existing DynamoDB tables please provide the corresponding values to the parameters (note that they will have to exist in the same region as the rest of the infrastructure).
-	Provide the names of the columns you would like to execute the DQ checks on and the extra parameters that are required for your checks.

After you finish click next.
 

Step 5:
In the next screen you can add Tags on your stack and use a specific IAM role if you want. You can also edit advanced options for Stack Policy, Rollback configuration etc. Click Next.
 

Step 6:
In the next screen you can review all your settings and make sure you tick the acknowledge button for IAM Role Creation before you click “Create stack”. After a few minutes and if you click on the “Stack info” tab you should be able to see the “CREATE_COMPLETE” green status.

 


Step 7:
Now please navigate to AWS Glue Service and then select “jobs” from the left panel. You will see that there is a newly created job called “jb-deeq-via-cfn1”. Please select this job, click “Action” and select “Run Job” as illustrated below.

  

 

A new pop up window will appear and select again “Run Job”.

Step 8:

Depending on the overall size of the data you have provided on your input data S3 bucket this job will take 10 min or more. When that is done you should see the “Run status” as “Succeeded” as shown below. When that is done please select AWS Dynamo DB. 

In the table “YourStackName-myInputDynamoDBTable” you should be able to see the list of your files you have provided as input. In the table “YourStackName- myOutputDynamoDBTable” you should be able to see the metrics for all your input data.
 

 

 

 
