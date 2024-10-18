"""
Test cases
"""

from databricks.sdk.runtime import dbutils
from databricks.sdk import AccountClient
from databricks.connect import DatabricksSession
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


def test_get_users():
    """
    Test get_users()
    """
    account_client = AccountClient(
        host = ACCOUNT_HOST,
        account_id = ACCOUNT_ID,
        azure_client_id = CLIENT_ID,
        azure_client_secret = CLIENT_SECRET,
        azure_tenant_id = TENANT_ID
    )

    dsd = DatabricksSystemData(
        spark = DatabricksSession.builder.getOrCreate(),
        account_client = account_client
        )

    cols = ['active',
            'displayname',
            'emails',
            'externalid',
            'id',
            'name',
            'username',
            'roles']

    assert dsd.get_users().columns == cols
