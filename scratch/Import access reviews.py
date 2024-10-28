# Databricks notebook source
dbutils.widgets.text("catalog", "auk_dataplatform")
dbutils.widgets.text("schema", "access_review")
dbutils.widgets.text("volumne_export", "access_review_export")
dbutils.widgets.text("volumne_import", "access_review_import")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volumne_export = dbutils.widgets.get("volumne_export")
volumne_import = dbutils.widgets.get("volumne_import")

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

file_schema = StructType(
    [
        StructField("_c0", IntegerType(), True),
        StructField("access_review_detail_id", IntegerType(), True),
        StructField("access_review_id", IntegerType(), True),
        StructField("outcome_id", IntegerType(), True),
        StructField("object_type", StringType(), True),
        StructField("object_name", StringType(), True),
        StructField("object_owner", StringType(), True),
        StructField("grantee", StringType(), True),
        StructField("catalog_name", StringType(), True),
        StructField("schema_name", StringType(), True),
        StructField("privilege_type", StringType(), True),
        StructField("is_grantable", StringType(), True),
        StructField("inherited_from", StringType(), True),
    ]
)

# df = (
#     spark.read.format("csv")
#     .option("pathGlobfilter", "*.csv")
#     .option("header", "true")
#     .schema(file_schema)
#     .load(f"/Volumes/{catalog}/{schema}/{volumne_import}/")
#     .drop("_c0")
#     .withColumn("import_timestamp", current_timestamp())
#     .withColumn("import_file_name", col("_metadata.file_name"))
# )

source = f"/Volumes/{catalog}/{schema}/{volumne_import}/"

@dlt.table(
  comment="Daily stream - access review data",
  table_properties={"quality": "bronze"}
)
def access_review_import():
      return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(file_schema)
            .load(f"{source}")
            .drop("_c0")
            .withColumn("import_timestamp", current_timestamp())
            .withColumn("import_file_name", col("_metadata.file_name"))
      )
