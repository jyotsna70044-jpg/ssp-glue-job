import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from lib.clean import clean_data
from lib.save_ops import write_in_s3_in_par
from lib.read_ops import read_from_s3_in_csv

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# external params
source_s3_path = args['S3_SOURCE_PATH'] if 'S3_SOURCE_PATH' in args else "s3://your-source-bucket/input-folder/"
print(source_s3_path)
target_s3_path = args['S3_TARGET_PATH'] if 'S3_TARGET_PATH' in args else "s3://your-target-bucket/output-folder/"
print(target_s3_path)
# ✅Step 1: Read data using s3 in csv
dynamic_frame = read_from_s3_in_csv(glueContext, source_s3_path)
dynamic_frame.printSchema()
# step 2: Convert DynamicFrame to Spark DataFrame for transformations
df = dynamic_frame.toDF()
df.head()
cleaned_df = clean_data(df)
transformed_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "dynamic_frame")
# Step 3: Write DynamicFrame to S3 in silver folder
write_in_s3_in_par(glueContext, target_s3_path, transformed_dyf)
# job end here
job.commit()
