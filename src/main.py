"""
Entry point databricks_system_data module  
"""

from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession
from databricks.sdk import AccountClient
from data_platform_tools.databricks_system_data import DatabricksSystemData

dbutils.widgets.text("catalog", "auk_dataplatform")
dbutils.widgets.text("schema", "system")

# --- CONFIG ----
ACCOUNT_HOST = "accounts.azuredatabricks.net"
TENANT_ID = "c3588c15-f840-4591-875f-b3d42610f22f"
ACCOUNT_ID = "42ba6f6a-250d-4e87-9433-3ab73685b3f6"
CLIENT_ID = "22a10d55-9e76-464d-96bc-3e6c3e44cc35"
CLIENT_SECRET = dbutils.secrets.get(scope="kv-redkic-ne-test", key="DatabricksAPI")
CATALOG_PATH = dbutils.widgets.get("catalog")
SCHEMA_PATH = dbutils.widgets.get("schema")
# --- END CONFIG ---


def get_spark() -> SparkSession:
    """
    Create a new Databricks Connect session.
    If this fails check that you have configured Databricks Connect correctly.

    See https://docs.databricks.com/dev-tools/databricks-connect.html.
    
    """
    try:
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

spark = get_spark()

def main():
    """Main entry point of wheel file"""

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_PATH}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_PATH}.{SCHEMA_PATH}")

    account_client = AccountClient(
        host = ACCOUNT_HOST,
        account_id = ACCOUNT_ID,
        azure_client_id = CLIENT_ID,
        azure_client_secret = CLIENT_SECRET,
        azure_tenant_id = TENANT_ID
    )

    dsd = DatabricksSystemData(
        spark = spark,
        account_client = account_client
        )

    df = dsd.get_runtime_versions()
    df.show()
    df = dsd.get_users()
    df.show()
    df = dsd.get_workspaces()
    df.show()
    df = dsd.get_workspace_privileges()
    df.show()
    df = dsd.get_clusters()
    df.show()
    df = dsd.get_warehouses()
    df.show()
    df = dsd.get_cluster_privileges()
    df.show()
    df = dsd.get_warehouse_privileges()
    df.show()


if __name__ == '__main__':
    main()
