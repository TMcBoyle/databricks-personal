from pyspark.sql import DataFrame, SparkSession
from typing import Any, Optional

def read_volume_csv(spark: SparkSession, path: str, watermarkColumn: Optional[str]=None, watermarkValue: Optional[Any]=None) -> DataFrame:
    """ Read data from an xlsx file into a dataframe """
    df = spark.read.format("csv").option("header", True).load(path)

    if watermarkColumn is not None and watermarkValue is not None:
        df = df.filter(df[watermarkColumn] > watermarkValue)

    return df
