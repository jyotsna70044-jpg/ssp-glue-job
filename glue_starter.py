import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from lib.processor import process
from lib.save_ops import write_in_redshift, write_in_s3_in_par
from lib.read_ops import read_from_catalog, read_from_s3_in_par, read_from_s3_in_csv

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_s3_path = args['S3_SOURCE_PATH'] if 'S3_SOURCE_PATH' in args else "s3://your-source-bucket/input-folder/"
print(source_s3_path)
# ✅Step-1: Read data using Glue Data Catalog
database_name = "athena_database1"
table_name = "empbronze"
dynamic_frame = read_from_catalog(glueContext, database_name, table_name)
# ✅Step-1B: Read data using s3 in csv
dynamic_frame = read_from_s3_in_csv(glueContext, source_s3_path)
# ✅Step-1C: Read data using s3 in Parquet
dynamic_frame = read_from_s3_in_par(glueContext, source_s3_path)

# step 2: Convert DynamicFrame to Spark DataFrame for transformations
df = dynamic_frame.toDF()
transformed_df = process(spark, df)
transformed_dyf = DynamicFrame.fromDF(transformed_df, glueContext, "dynamic_frame")

# Step 3a: Write DynamicFrame to Redshift
redshift_temp_dir = "s3://jyotsna-deltalake/glue_temp/"
dbtable = "dev.public.employee"
database = "dev"
write_in_redshift(glueContext, dbtable, database, transformed_dyf, redshift_temp_dir)
# Step 3b: Write DynamicFrame to S3
target_s3_path = args['S3_TARGET_PATH'] if 'S3_TARGET_PATH' in args else "s3://your-target-bucket/output-folder/"
print(target_s3_path)
write_in_s3_in_par(glueContext, target_s3_path, transformed_dyf)
# job end here
job.commit()
