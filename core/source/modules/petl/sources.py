from enum import Enum
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, List, Dict, Callable

class DatabricksVolume:
    def __init__(self, volume_path: str):
        self.volume_path = volume_path

    def get_data(self, spark: SparkSession, format: str, path: str,
                 options: Optional[Dict]=None,
                 filters: Optional[List[Callable]]=None) -> DataFrame:
        df = spark.read.format(format)

        if options is not None:
            for option, value in options.items():
                df = df.option(option, value)

        if filters is not None:
            for filter in filters:
                df = filter(df)
        
        return df.load(path)
