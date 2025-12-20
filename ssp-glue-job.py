import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

INPUT_PATH="click-s3-glue-redshift/bronze/"
OUTPUT_PATH="silver/combined_data.csv"
# Define input and output paths from job arguments
input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']
dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [INPUT_PATH]},
        format="csv",  # Example: 'csv', 'json', 'parquet'
        format_options={"withHeader": True, "separator": ","} if format == "csv" else {},  # Example for CSV with header
        transformation_ctx="datasource0"
    )

df = dynamic_frame.toDF()
print('HI')
# # 2. Read the CSV data from S3 into a Spark DataFrame
# # Using the Spark DataFrame directly often provides more flexibility for cleaning operations
# df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load(input_path)

print(f"Original DataFrame schema:")
df.printSchema()

# 3. Perform Data Cleaning Operations
# ---
# Example 1: Drop unnecessary columns
# columns_to_drop = ['unwanted_col_1', 'unwanted_col_2']
# df = df.drop(*columns_to_drop)

# Example 2: Handle missing values (e.g., fill with a default value or drop rows)
# Fill nulls in a specific string column with 'Unknown'
df = df.fillna('Unknown', subset=['DepartmentID'])
# Drop rows with nulls in critical columns
df = df.dropna(subset=['Name'])

# Example 3: Correct data types (if inferSchema didn't work as expected)
# Cast a column to an integer type
df = df.withColumn("Salary", col("Salary").cast("float"))

# Example 4: Filter rows based on specific criteria
# Filter out rows where 'status' column has the value 'unknown'
# df = df.filter(df['status'] != 'unknown')

# Example 5: Rename columns
df = df.withColumnRenamed("DepartmentID", "Department")

# Example 6: Create a new column based on existing data
# df = df.withColumn("is_active_status", when(col("status") == "active", True).otherwise(False))

print(f"Cleaned DataFrame schema:")
df.printSchema()

transformed_dyf = dynamic_frame.fromDF(df, glueContext, "dynamic_frame")
glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": OUTPUT_PATH},
        format="parquet",  # Example: 'parquet', 'csv', 'json'
        format_options={"compression": "snappy"} if format == "parquet" else {},  # Example for Parquet compression
        transformation_ctx="datasink0"
    )


# 4. Write the cleaned data to the target S3 bucket (e.g., in Parquet format for better performance)
# Parquet is generally recommended for analytics workloads
# df.write.format("parquet") \
#     .mode("overwrite") \
#     .save(output_path)

job.commit()
