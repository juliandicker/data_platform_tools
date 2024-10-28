"""
Entry point databricks_system_data module  
"""

from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession
from databricks.sdk import AccountClient
from data_platform_tools.databricks_system_data import DatabricksSystemData

dbutils.widgets.text("tenant", "c3588c15-f840-4591-875f-b3d42610f22f")
dbutils.widgets.text("account_id", "42ba6f6a-250d-4e87-9433-3ab73685b3f6")
dbutils.widgets.text("client_id", "22a10d55-9e76-464d-96bc-3e6c3e44cc35")

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

    account_client = AccountClient(
        host = "accounts.azuredatabricks.net",
        account_id = dbutils.widgets.get("account_id"),
        azure_client_id = dbutils.widgets.get("client_id"),
        azure_client_secret = dbutils.secrets.get(scope="kv-redkic-ne-test", key="DatabricksAPI"),
        azure_tenant_id = dbutils.widgets.get("tenant_id")
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
