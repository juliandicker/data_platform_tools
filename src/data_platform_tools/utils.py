"""
Module providing utility functions to convert objects to dictionary.
"""

import re
import pprint
from pyspark.sql import SparkSession, DataFrame
from databricks.sdk import AccountClient, WorkspaceClient

def to_snake(s):
    """
    Convert string to snake case
    """
    return re.sub("(\\w*)[-\\s](\\w*)", "\\1_\\2", s).lower()

def to_dict(d):
    """
    Recursivly converts a dictionary or list to a list of dictionaries
    """
    if isinstance(d, list):
        return [to_dict(i) if isinstance(i, (dict, list)) else i for i in d]
    return {
        to_snake(a): to_dict(b) if isinstance(b, (dict, list)) else b
        for a, b in d.items()
    }

def object_to_dict(obj):
    """
    Convert an list of objects to list of dictionaries
    """
    return [to_dict(i.as_dict()) for i in obj]

def object_to_dataframe(spark: SparkSession, obj, schema = None) -> DataFrame:
    """
    Convert an list of objects to a dataframe
    """

    # pprint.pprint(obj)
    objdict = object_to_dict(obj)
    # pprint.pprint(objdict)
    # print()

    if len(objdict) == 0:
        return None
    df = spark.createDataFrame(objdict, schema)
    return df

def get_workspace_client(
        account_client: AccountClient,
        workspace_deployment_name
        ) -> WorkspaceClient:
    """
    Returns a WorkspaceClient using AccountClient credentials
    """

    return WorkspaceClient(
        host=f"https://{workspace_deployment_name}.azuredatabricks.net",
        azure_client_id = account_client.config.azure_client_id,
        azure_client_secret = account_client.config.azure_client_secret,
        azure_tenant_id = account_client.config.azure_tenant_id
    )
