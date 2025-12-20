def write_in_redshift(glue_context, dbtable, database, dyf, redshift_temp_dir):
    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-connection",  # Glue Redshift connection
        connection_options={
            "dbtable": "dev.public.employee",  # Target Redshift table
            "database": "dev"
        },
        redshift_tmp_dir=redshift_temp_dir,
        transformation_ctx="redshift_sink"
    )

def write_in_s3_in_csv(glue_context, target_s3_path, dyf):
    glue_context.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": target_s3_path,
            "compression": "gzip"
        },
        format="csv",
        format_options={
            "writeHeader": True,
            "separator": ","
        }
    )

def write_in_s3_in_par(glue_context, target_s3_path, dyf):
    glue_context.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={"path": target_s3_path},
        format="parquet",  # Example: 'parquet', 'csv', 'json'
        format_options={"compression": "snappy"} if format == "parquet" else {},  # Example for Parquet compression
        transformation_ctx="datasink0"
    )

