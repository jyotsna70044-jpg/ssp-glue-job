def read_from_s3_in_csv(spark, source_input_path):
    df = spark.read.format("csv").options(header="True", inferSchema="True").load(source_input_path)
    return df


def read_from_s3_in_par(spark, source_input_path):
    df = spark.read.format("parquet").options(header="True", inferSchema="True").load(source_input_path)
    return df
