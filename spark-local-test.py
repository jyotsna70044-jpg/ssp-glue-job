import findspark
findspark.init()
from pyspark.sql import SparkSession

from clean import clean_data
from local_read_ops import read_from_s3_in_csv,read_from_s3_in_par
from local_save_ops import write_in_s3_in_par,write_in_s3_in_csv

# Create a SparkSession
spark = SparkSession.builder.appName('bronze data').getOrCreate()

# external params
source_input_path="data/bronze/combined_data.csv"
target_output_path="data/silver/combined_data.csv"
# step1: Read Csv file
df =read_from_s3_in_csv(spark, source_input_path)
df.printSchema()
# step2: clean  data
cleaned_df = clean_data(df)
# step3: save  data
# write_in_s3_in_par(cleaned_df,target_output_path)
write_in_s3_in_csv(cleaned_df,target_output_path)
