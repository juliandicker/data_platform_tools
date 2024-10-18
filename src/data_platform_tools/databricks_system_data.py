"""
Module to gather complementary system table data for databricks
"""

from io import StringIO
import requests
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, MapType, BooleanType, FloatType
from pyspark.sql.functions import lit
from databricks.sdk import AccountClient, WorkspaceClient
from bs4 import BeautifulSoup
from . import utils

class DatabricksSystemData:
    """
    Wrapper of databricks.sdk to return dataframe
    """

    def __init__(self,
                 spark: SparkSession,
                 account_client: AccountClient):
        self.account_client = account_client
        self.spark = spark

    def get_runtime_versions(self) -> DataFrame:
        """
        Scrapes Databricks Runtime from
        https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/
        """

        response = requests.get(
            "https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/"
            , timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        table_html = (soup
                    .find(id="all-supported-databricks-runtime-releases")
                    .find_next_sibling("table")
                    )
        df = pd.read_html(StringIO(str(table_html)))[0]
        df["Release date"] = pd.to_datetime(df["Release date"])
        df["End-of-support date"] = pd.to_datetime(df["End-of-support date"])

        df.columns = [utils.to_snake(c) for c in df.columns]
        return self.spark.createDataFrame(df)


    def get_workspaces(self) -> DataFrame:
        """
        List the databricks workspaces
        """
        return utils.object_to_dataframe(self.spark, self.account_client.workspaces.list())


    def get_workspace_privileges(self) -> DataFrame:
        """
        List the databricks all workspaces and privileges
        """

        schema = StructType([
            StructField("permissions", StringType(), True),
            StructField("principal", StringType(), True),
            StructField("workspace_id", IntegerType(), True)
        ])

        df_all = self.spark.createDataFrame([], schema=schema)

        for workspace in self.account_client.workspaces.list():

            wa_list = (self
                       .account_client.workspace_assignment
                       .list(workspace_id=workspace.workspace_id))

            df = (utils
                  .object_to_dataframe(self.spark, wa_list)
                  .withColumn("workspace_id", lit(workspace.workspace_id))
                  )

            if df_all.isEmpty():
                df_all = df
            else:
                df_all = df_all.union(df)

        return df_all


    def get_users(self) -> DataFrame:
        """
        List the databricks users
        """

        schema = StructType([
                    StructField('emails', ArrayType(StructType([
                        StructField('primary', BooleanType(), True),
                        StructField('type', StringType(), True),
                        StructField('value', StringType(), True)
                        ])), True),
                    StructField('active', BooleanType(), True),
                    StructField('displayname', StringType(), True),
                    StructField('externalid', StringType(), True),
                    StructField('id', StringType(), True),
                    StructField('name', StructType([
                        StructField('familyname', StringType(), True),
                        StructField('givenname', StringType(), True)
                        ]), True),
                    StructField('roles', ArrayType(StructType([
                        StructField('type', StringType(), True),
                        StructField('value', StringType(), True)
                        ])), True),
                    StructField('username', StringType(), True)
                ])

        return utils.object_to_dataframe(self.spark, self.account_client.users.list(), schema)


    def _get_workspace_client(self, workspace_deployment_name):
        return WorkspaceClient(
            host=f"https://{workspace_deployment_name}.azuredatabricks.net",
            azure_client_id = self.account_client.config.azure_client_id,
            azure_client_secret = self.account_client.config.azure_client_secret,
            azure_tenant_id = self.account_client.config.azure_tenant_id
        )

    def _union(self, df, li, schema = None) -> DataFrame:
        df_new = utils.object_to_dataframe(self.spark, li, schema)

        if df_new is not None:
            df = df.unionByName(df_new, allowMissingColumns=True)

        return df

    def get_warehouses(self) -> DataFrame:
        """
        List the databricks warehouses
        """
        schema = StructType([
                    StructField('auto_stop_mins', IntegerType(), True),
                    StructField('creator_name', StringType(), True),
                    StructField('enable_photon', BooleanType(), True),
                    StructField('enable_serverless_compute', BooleanType(), True),
                    StructField('id', StringType(), True),
                    StructField('jdbc_url', StringType(), True),
                    StructField('max_num_clusters', IntegerType(), True),
                    StructField('min_num_clusters', IntegerType(), True),
                    StructField('num_clusters', IntegerType(), True),
                    StructField('name', StringType(), True),
                    StructField('num_active_sessions', IntegerType(), True),
                    StructField('odbc_params', StructType([
                        StructField('hostname', StringType(), True),
                        StructField('path', StringType(), True),
                        StructField('port', IntegerType(), True),
                        StructField('protocol', StringType(), True)
                        ]), True),
                    StructField('spot_instance_policy', StringType(), True),
                    StructField('state', StringType(), True),
                    StructField('tags', StructType([
                        StructField('custom_tags', ArrayType(MapType(StringType(), StringType())), True)
                        ]), True),
                    StructField('warehouse_type', StringType(), True)
                ])


        df_all = self.spark.createDataFrame([], "id STRING")

        for workspace in self.account_client.workspaces.list():
            workspace_client = self._get_workspace_client(workspace.deployment_name)
            df_all = (self
                      ._union(df_all, workspace_client.warehouses.list(), schema)
                      .withColumn("workspace_id", lit(workspace.workspace_id))
                      )

        return df_all

    def get_clusters(self) -> DataFrame:
        """
        List the databricks clusters
        """

        schema = StructType([
                    StructField('num_workers', IntegerType(), True),
                    StructField('autoscale', MapType(StringType(), IntegerType()), True),
                    StructField('cluster_name', StringType(), True),
                    StructField('spark_version', StringType(), True),
                    StructField('spark_conf', MapType(StringType(), StringType()), True),
                    StructField('azure_attributes', StructType([
                        StructField('log_analytics_info', MapType(StringType(), StringType()), True),
                        StructField('first_on_demand', IntegerType(), True),
                        StructField('availability', StringType(), True),
                        StructField('spot_bid_max_price', FloatType(), True)
                        ]), True),
                    StructField('node_type_id', StringType(), True),
                    StructField('driver_node_type_id', StringType(), True),
                    StructField('ssh_public_keys', ArrayType(StringType()), True),
                    StructField('custom_tags', MapType(StringType(), StringType()), True),
                    StructField('spark_env_vars', MapType(StringType(), StringType()), True),
                    StructField('autotermination_minutes', IntegerType(), True),
                    StructField('enable_elastic_disk', BooleanType(), True),
                    StructField('instance_pool_id', StringType(), True),
                    StructField('policy_id', StringType(), True),
                    StructField('enable_local_disk_encryption', BooleanType(), True),
                    StructField('driver_instance_pool_id', StringType(), True),
                    StructField('workload_type', StructType([
                        StructField('clients', MapType(StringType(), StringType()), True)
                        ]), True),
                    StructField('runtime_engine', StringType(), True),
                    StructField('data_security_mode', StringType(), True),
                    StructField('single_user_name', StringType(), True),
                    StructField('cluster_id', StringType(), True),
                    StructField('cluster_source', StringType(), True),
                    StructField('creator_user_name', StringType(), True),
                    StructField('driver', StructType([
                        StructField('host_private_ip', StringType(), True),
                        StructField('private_ip', StringType(), True),
                        StructField('public_dns', StringType(), True),
                        StructField('node_id', StringType(), True),
                        StructField('instance_id', StringType(), True),
                        StructField('start_timestamp', LongType(), True),
                        ]), True),
                    StructField('executors', ArrayType(StructType([
                        StructField('host_private_ip', StringType(), True),
                        StructField('private_ip', StringType(), True),
                        StructField('public_dns', StringType(), True),
                        StructField('node_id', StringType(), True),
                        StructField('instance_id', StringType(), True),
                        StructField('start_timestamp', LongType(), True),
                        ])), True),
                    StructField('spark_context_id', LongType(), True),
                    StructField('jdbc_port', IntegerType(), True),
                    StructField('state', StringType(), True),
                    StructField('state_message', StringType(), True),
                    StructField('start_time', LongType(), True),
                    StructField('terminated_time', LongType(), True),
                    StructField('last_state_loss_time', LongType(), True),
                    StructField('last_restarted_time', LongType(), True),
                    StructField('cluster_memory_mb', IntegerType(), True),
                    StructField('cluster_cores', FloatType(), True),
                    StructField('default_tags', MapType(StringType(), StringType()), True),
                    StructField('cluster_log_status', StructType([
                        StructField('last_attempted', LongType(), True),
                        StructField('last_exception', StringType(), True)
                        ]), True),
                    StructField('termination_reason', StructType([
                        StructField('code', StringType(), True),
                        StructField('type', StringType(), True),
                        StructField('parameters', MapType(StringType(), StringType()), True),
                        ]), True)
                ])

        df_all = self.spark.createDataFrame([], "cluster_id STRING")

        for workspace in self.account_client.workspaces.list():
            workspace_client = self._get_workspace_client(workspace.deployment_name)
            df_all = (self
                      ._union(df_all, workspace_client.clusters.list(), schema)
                      .withColumn("workspace_id", lit(workspace.workspace_id))
                      )

        return df_all


    def _get_privileges(self, workspace_client, request_object_type, object_key, object_id):
        """
        Sub function to list privileges for a databricks object type
        """

        schema = StructType([
                    StructField('all_permissions', ArrayType(StructType([
                        StructField('inherited', BooleanType(), True),
                        StructField('permission_level', StringType(), True),
                        StructField('inherited_from_object', ArrayType(StringType()), True)
                        ])), True),
                    StructField('display_name', StringType(), True),
                    StructField('user_name', StringType(), True),
                    StructField('group_name', StringType(), True)
                ])

        permissions = workspace_client.permissions.get(request_object_type, object_id)

        return (utils
                .object_to_dataframe(self.spark, permissions.access_control_list, schema)
                .withColumn(object_key, lit(object_id))
                )


    def get_cluster_privileges(self):
        """
        List the all databricks cluster privileges
        """
        request_object_type = "clusters"
        object_key = "cluster_id"
        df_all = self.spark.createDataFrame([], f"{object_key} STRING")

        for workspace in self.account_client.workspaces.list():
            workspace_client = self._get_workspace_client(workspace.deployment_name)

            for cluster in workspace_client.clusters.list():
                df_all = df_all.unionByName(
                    self._get_privileges(
                        workspace_client,
                        request_object_type,
                        object_key,
                        cluster.cluster_id),
                    allowMissingColumns=True)

            df_all = df_all.withColumn("workspace_id", lit(workspace.workspace_id))

        return df_all

    def get_warehouse_privileges(self):
        """
        List the all databricks warehouse privileges
        """
        request_object_type = "warehouses"
        object_key = "warehouse_id"
        df_all = self.spark.createDataFrame([], f"{object_key} STRING")

        for workspace in self.account_client.workspaces.list():
            workspace_client = self._get_workspace_client(workspace.deployment_name)

            for warehouse in workspace_client.warehouses.list():
                df_all = df_all.unionByName(
                    self._get_privileges(
                        workspace_client,
                        request_object_type,
                        object_key,
                        warehouse.id),
                    allowMissingColumns=True)

            df_all = df_all.withColumn("workspace_id", lit(workspace.workspace_id))

        return df_all
