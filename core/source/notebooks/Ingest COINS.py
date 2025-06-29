import os, sys
sys.path.append(os.path.abspath("../modules/"))

from petl.ingestion import read_volume_csv

config = [
    {
        "entity": "coins_company",
        "source_system": "coins",
        "source_type": "volume_csv",
        "connection_details": {
            "volume_path": "/Volumes/source/data/files/COINS_company.csv"
        },
        "watermark": "jtlastupdate"
    },
    {
        "entity": "coins_development",
        "source_system": "coins",
        "source_type": "volume_csv",
        "connection_details": {
            "volume_path": "/Volumes/source/data/files/COINS_development.csv"
        },
        "watermark": "jtlastupdate"
    },
    {
        "entity": "coins_plot",
        "source_system": "coins",
        "source_type": "volume_csv",
        "connection_details": {
            "volume_path": "/Volumes/source/data/files/COINS_plot.csv"
        },
        "watermark": "jtlastupdate"
    },
    {
        "entity": "coins_sales",
        "source_system": "coins",
        "source_type": "volume_csv",
        "connection_details": {
            "volume_path": "/Volumes/source/data/files/COINS_sales.csv"
        },
        "watermark": "jtlastupdate"
    }
]

for entity in config:
    if entity['source_type'] == 'volume_csv':
        max_watermark = None
        if entity['watermark'] != "null":
            try:
                max_watermark = spark.sql(f"SELECT MAX({entity['watermark']}) FROM bronze.{entity['source_system']}.{entity['entity']}").collect()[0][0]
            except:
                pass

        df = read_volume_csv(spark, entity['connection_details']['volume_path'], entity['watermark'], max_watermark)

    display(df)
    #df.write.mode("append").option("mergeSchema", True).saveAsTable(f"bronze.{entity['source_system']}.{entity['entity']}")
