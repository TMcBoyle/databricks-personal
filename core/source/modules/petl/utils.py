from pyspark.sql import SparkSession

def get_max_column_value(spark: SparkSession, table: str, column_name: str):
    return spark.sql(f"""
                SELECT 
                    MAX({column_name})
                FROM {table}.{column_name}
            """).collect()[0][0]
