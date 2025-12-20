def read_from_catalog(glue_context, database_name, table_name):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx="datasource"
    )
    return dynamic_frame


def read_from_s3_in_csv(glue_context, source_s3_path):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_s3_path]},
        format="csv",  # Example: 'csv', 'json', 'parquet'
        format_options={"withHeader": True, "separator": ","} if format == "csv" else {},  # Example for CSV with header
        transformation_ctx="datasource0"
    )
    return dynamic_frame


def read_from_s3_in_par(glue_context, source_s3_path):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_s3_path]},
        format="parquet",  # Example: 'csv', 'json', 'parquet'
        # not required# format_options={"withHeader": True, "separator": ","} if format == "csv" else {},  # Example for CSV with header
        transformation_ctx="datasource0"
    )
    return dynamic_frame
