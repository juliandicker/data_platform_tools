-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "auk_dataplatform")
-- MAGIC dbutils.widgets.text("schema", "access_review")
-- MAGIC dbutils.widgets.text("volumne_export", "access_review_export")
-- MAGIC dbutils.widgets.text("volumne_import", "access_review_import")
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC volumne_export = dbutils.widgets.get("volumne_export")
-- MAGIC volumne_import = dbutils.widgets.get("volumne_import")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
-- MAGIC spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volumne_export}")
-- MAGIC spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volumne_import}")
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS access_review_outcomes (
  outcome_id INT PRIMARY KEY,
  outcome_value STRING NOT NULL
);

CREATE OR REPLACE TEMP VIEW new_review_outcomes(outcome_id, outcome_value) AS VALUES
(1, 'Approved'),
(2, 'Denied'),
(3, 'Not reviewed'),
(4, 'Don\'t know');

MERGE INTO access_review_outcomes a
USING new_review_outcomes b
ON a.outcome_id=b.outcome_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS access_reviews (
  access_review_id BIGINT GENERATED ALWAYS AS IDENTITY,
  created_date TIMESTAMP,
  created_by STRING
);

INSERT INTO access_reviews (created_date, created_by)
VALUES (current_timestamp(), current_user());

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS access_review_detail (
  access_review_detail_id BIGINT GENERATED ALWAYS AS IDENTITY,
  access_review_id BIGINT,
  outcome_id INT,
  object_type STRING,
  object_name STRING,
  object_owner STRING,
  grantee STRING,
  catalog_name STRING,
  schema_name STRING,
  privilege_type STRING,
  is_grantable STRING,
  inherited_from STRING
);

WITH access_reviews_cte AS
(
    SELECT MAX(access_review_id) as max_access_reviews_id
    FROM access_reviews
)

INSERT INTO access_review_detail (
  access_review_id,
  outcome_id,
  object_type,
  object_name,
  object_owner,
  grantee,
  catalog_name,
  schema_name,
  privilege_type,
  is_grantable,
  inherited_from)
SELECT 
  max_access_reviews_id as access_review_id,
  3 as outcome_id,
  object_type,
  object_name,
  object_owner,
  grantee,
  catalog_name,
  schema_name,
  privilege_type,
  is_grantable,
  inherited_from
 FROM auk_dataplatform.system.all_privileges, access_reviews_cte

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import unicodedata
-- MAGIC import re
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC def slugify(value, allow_unicode=False):
-- MAGIC     """
-- MAGIC     Taken from https://github.com/django/django/blob/master/django/utils/text.py
-- MAGIC     Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
-- MAGIC     dashes to single dashes. Remove characters that aren't alphanumerics,
-- MAGIC     underscores, or hyphens. Convert to lowercase. Also strip leading and
-- MAGIC     trailing whitespace, dashes, and underscores.
-- MAGIC     """
-- MAGIC     value = str(value)
-- MAGIC     if allow_unicode:
-- MAGIC         value = unicodedata.normalize('NFKC', value)
-- MAGIC     else:
-- MAGIC         value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
-- MAGIC     value = re.sub(r'[^\w\s-]', '', value.lower())
-- MAGIC     return re.sub(r'[-\s]+', '-', value).strip('-_')
-- MAGIC
-- MAGIC
-- MAGIC def write_access_reviews_to_csv():
-- MAGIC     max_access_review_id = (spark
-- MAGIC                             .read
-- MAGIC                             .format("delta")
-- MAGIC                             .table(f"{catalog}.{schema}.access_reviews")
-- MAGIC                             .agg({"access_review_id": "max"}).collect()[0][0]
-- MAGIC                             )
-- MAGIC
-- MAGIC     df = (spark
-- MAGIC         .read
-- MAGIC         .format("delta")
-- MAGIC         .table(f"{catalog}.{schema}.access_review_detail")
-- MAGIC         .filter(col("access_review_id") == max_access_review_id)
-- MAGIC         )
-- MAGIC
-- MAGIC
-- MAGIC     # Grouping the DataFrame by 'object_owner'
-- MAGIC     grouped_df = df.groupBy("object_owner").count()
-- MAGIC
-- MAGIC     # Iterating over each distinct 'object_owner' to write their data to separate CSV files
-- MAGIC     distinct_owners = [row.object_owner for row in grouped_df.select("object_owner").distinct().collect()]
-- MAGIC     for owner in distinct_owners:
-- MAGIC         owner_df = df.filter(col("object_owner") == owner)
-- MAGIC         filename = slugify(f"{max_access_review_id}_{owner}")
-- MAGIC         owner_df.toPandas().to_csv(f"/Volumes/{catalog}/{schema}/{volumne_export}/{filename}.csv")
-- MAGIC
-- MAGIC write_access_reviews_to_csv()
