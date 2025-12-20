from pyspark.sql.functions import col, when
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import DoubleType


def clean_data(df):
    # Fill nulls in a specific string column with 'Unknown'
    df = df.fillna('Unknown', subset=['DepartmentID'])
    # Drop rows with nulls in critical columns
    df = df.dropna(subset=['Name'])
    # Cast a column to an integer type
    df = df.withColumn(
        "price_cleaned",
        regexp_replace(regexp_replace(col("Salary"), "\\$", ""), ",", "").cast(DoubleType())
    )
    # Filter out rows where 'status' column has the value 'unknown'
    # df = df.filter(df['status'] != 'unknown')
    # Example 5: Rename columns
    df = df.withColumnRenamed("DepartmentID", "Department")
    # Example 6: Create a new column based on existing data
    # df = df.withColumn("is_active_status", when(col("status") == "active", True).otherwise(False))
    print(f"Cleaned DataFrame schema:")
    df.printSchema()
    return df
