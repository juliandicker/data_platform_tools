"""
Entry point databricks_system_data module  
"""

from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession
from databricks.sdk import AccountClient
from data_platform_tools.databricks_system_data import DatabricksSystemData

# --- CONFIG ----
ACCOUNT_HOST = "accounts.azuredatabricks.net"
TENANT_ID = "c3588c15-f840-4591-875f-b3d42610f22f"
ACCOUNT_ID = "42ba6f6a-250d-4e87-9433-3ab73685b3f6"
CLIENT_ID = "22a10d55-9e76-464d-96bc-3e6c3e44cc35"
CLIENT_SECRET = dbutils.secrets.get(scope="kv-redkic-ne-test", key="DatabricksAPI")
CATALOG_PATH = "auk_dataplatform"
SCHEMA_PATH = "system"
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

    # dsd.test2().show(20, False)

    # dsd.get_workspaces().show(20, False)
    # dsd.get_users().show(20, False)
    # dsd.get_warehouses().show(20, False)
    dsd.get_clusters().show()
    # dsd.get_workspaces_privileges().show(20, False)
    # dsd.get_cluster_privileges().show(20, False)
    # dsd.get_warehouses_privileges().show(20, False)



if __name__ == '__main__':
    main()
