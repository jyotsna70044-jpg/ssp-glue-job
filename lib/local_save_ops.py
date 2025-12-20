def write_in_s3_in_csv(df, target_output_path):
    # df.write.option("header", True).mode("overwrite").csv(target_output_path)
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(target_output_path)


def write_in_s3_in_par(df, target_output_path):
    df.write.option("header", True).mode("overwrite").parquet(target_output_path)
    df.coalesce(1).write.option("header", True).mode("overwrite").parquet(target_output_path)


