from pyspark.sql.functions import col


def process(spark, df):
    df_clean = df.na.drop(subset=["DepartmentID"])
    df_grp = df_clean.groupby('DepartmentID').agg(sum(col('Salary')))
    df_grp.show()
