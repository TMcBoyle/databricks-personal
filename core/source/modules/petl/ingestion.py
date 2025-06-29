from pyspark.sql import DataFrame, SparkSession

def read_volume_csv(spark: SparkSession, path: str) -> DataFrame:
    """ Read data from an xlsx file into a dataframe """
    df = spark.read.format("csv").load(path)
    return df
