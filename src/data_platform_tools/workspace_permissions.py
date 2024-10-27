"""
Module to gather permission data for databricks workspaces
"""

from abc import ABC, abstractmethod
from typing import TypeVar, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as ST
import pyspark.sql.functions as F
from databricks.sdk import AccountClient, WorkspaceClient
from . import utils

T = TypeVar('T')

class WorkspacePermissionsBase(ABC):
    """
    Abstract class to gather permission data for databricks workspaces
    """
    def __init__(self,
                 spark: SparkSession,
                 account_client: AccountClient):
        self.account_client = account_client
        self.spark = spark

    @abstractmethod
    def get_workspace_object_list(self,
                                  workspace_client: WorkspaceClient
                                  ) -> List[T]:
        """
        Gets the workspace sub object list
        """

    @abstractmethod
    def get_object_id(self, workspace_object):
        """
        Gets the workspace sub object list
        """

    @property
    def request_object_type(self):
        """
        Returns the request object type
        """
        return ""

    @property
    def object_key(self):
        """
        Returns the object key
        """
        return ""

    def _tranfsorm_permissions(self, df: DataFrame) -> DataFrame:

        drop = ("all_permissions", "permission_level",
                "user_name", "service_principal_name", "group_name")

        return (df
                .withColumn("inherited", F.col("all_permissions")[0].inherited)
                .withColumn("inherited_from_object",
                            F.col("all_permissions")[0].inherited_from_object)
                .withColumn("permission_level", F.col("all_permissions")[0].permission_level)
                .withColumn("privilege_type", F.col("permission_level"))
                .withColumn("type",
                    F.when(df.group_name.isNotNull(), F.lit("Group"))
                    .when(df.service_principal_name.isNotNull(), F.lit("Service Principal"))
                    .when(df.user_name.isNotNull(), F.lit("User"))
                    .otherwise(F.lit("UNKNOWN")))
                .withColumn("display_name",
                    F.when(F.col("type") == "Group", df.group_name)
                    .when(F.col("type") == "User", df.user_name)
                    .otherwise(df.display_name))
                .drop(*drop)
                )

    def get_permissions(self) -> DataFrame:
        """
        List the all databricks object permissions
        """

        schema = ST.StructType([
            ST.StructField(self.object_key, ST.StringType(), True),
            ST.StructField('all_permissions', ST.ArrayType(ST.StructType([
                ST.StructField('inherited', ST.BooleanType(), True),
                ST.StructField('permission_level', ST.StringType(), True),
                ST.StructField('inherited_from_object', ST.ArrayType(ST.StringType()), True)
                ])), True),
            ST.StructField('display_name', ST.StringType(), True),
            ST.StructField('user_name', ST.StringType(), True),
            ST.StructField('service_principal_name', ST.StringType(), True),
            ST.StructField('group_name', ST.StringType(), True)
        ])

        df_all = self.spark.createDataFrame([], schema)

        for workspace in self.account_client.workspaces.list():
            workspace_client = utils.get_workspace_client(self.account_client,
                                                          workspace.deployment_name)

            for workspace_object in self.get_workspace_object_list(workspace_client):
                object_id = self.get_object_id(workspace_object)
                permissions = workspace_client.permissions.get(self.request_object_type, object_id)

                df = (
                    utils
                    .object_to_dataframe(self.spark, permissions.access_control_list, schema)
                    .withColumn(self.object_key, F.lit(object_id))
                    )

                df_all = df_all.unionByName(df, allowMissingColumns=True)

            df_all = df_all.withColumn("workspace_id", F.lit(workspace.workspace_id))

        return self._tranfsorm_permissions(df_all)

class ClusterPermissions(WorkspacePermissionsBase):
    """
    Concrete class to gather cluster permission data for databricks workspaces
    """

    @property
    def request_object_type(self):
        return "clusters"

    @property
    def object_key(self):
        return "cluster_id"

    def get_workspace_object_list(self, workspace_client) -> List[T]:
        return workspace_client.clusters.list()

    def get_object_id(self, workspace_object):
        return workspace_object.cluster_id

class WarehousePermissions(WorkspacePermissionsBase):
    """
    Concrete class to gather warehouse permission data for databricks workspaces
    """

    @property
    def request_object_type(self):
        return "warehouses"

    @property
    def object_key(self):
        return "warehouse_id"

    def get_workspace_object_list(self, workspace_client) -> List[T]:
        return workspace_client.warehouses.list()

    def get_object_id(self, workspace_object):
        return workspace_object.id
