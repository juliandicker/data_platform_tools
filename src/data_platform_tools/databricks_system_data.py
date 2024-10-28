"""
Module to gather complementary system table data for databricks
"""

from io import StringIO
import requests
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as ST
import pyspark.sql.functions as F
from databricks.sdk import AccountClient
from bs4 import BeautifulSoup
from . import utils, workspace_permissions

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

        schema = ST.StructType([
            ST.StructField('account_id', ST.StringType(), True),
            # ST.StructField('azure_workspace_info', ST.MapType(ST.StringType(), ST.StringType(), True), True),
            ST.StructField('creation_time', ST.LongType(), True),
            ST.StructField('deployment_name', ST.StringType(), True),
            ST.StructField('location', ST.StringType(), True),
            ST.StructField('pricing_tier', ST.StringType(), True),
            ST.StructField('workspace_id', ST.LongType(), True),
            ST.StructField('workspace_name', ST.StringType(), True),
            # ST.StructField('workspace_status', ST.StringType(), True),
            # ST.StructField('workspace_status_message', ST.StringType(), True)
        ])
        return utils.object_to_dataframe(self.spark, self.account_client.workspaces.list(), schema)

    def get_workspace_privileges(self) -> DataFrame:
        """
        List the databricks all workspaces and privileges
        """

        schema = ST.StructType([
            ST.StructField("permissions", ST.StringType(), True),
            ST.StructField("principal", ST.StringType(), True),
            ST.StructField("workspace_id", ST.IntegerType(), True)
        ])

        df_all = self.spark.createDataFrame([], schema=schema)

        for workspace in self.account_client.workspaces.list():

            wa_list = (self
                       .account_client.workspace_assignment
                       .list(workspace_id=workspace.workspace_id))

            df = (utils
                  .object_to_dataframe(self.spark, wa_list)
                  .withColumn("workspace_id", F.lit(workspace.workspace_id))
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

        schema = ST.StructType([
                    ST.StructField('emails', ST.ArrayType(ST.StructType([
                        ST.StructField('primary', ST.BooleanType(), True),
                        ST.StructField('type', ST.StringType(), True),
                        ST.StructField('value', ST.StringType(), True)
                        ])), True),
                    ST.StructField('active', ST.BooleanType(), True),
                    ST.StructField('displayname', ST.StringType(), True),
                    ST.StructField('externalid', ST.StringType(), True),
                    ST.StructField('id', ST.StringType(), True),
                    ST.StructField('name', ST.StructType([
                        ST.StructField('familyname', ST.StringType(), True),
                        ST.StructField('givenname', ST.StringType(), True)
                        ]), True),
                    ST.StructField('roles', ST.ArrayType(ST.StructType([
                        ST.StructField('type', ST.StringType(), True),
                        ST.StructField('value', ST.StringType(), True)
                        ])), True),
                    ST.StructField('username', ST.StringType(), True)
                ])

        return utils.object_to_dataframe(self.spark, self.account_client.users.list(), schema)


    def _union(self, df, li, schema = None, add_cols = None) -> DataFrame:
        df_new = utils.object_to_dataframe(self.spark, li, schema)

        if li:
            if add_cols is not None:
                for k, v in add_cols.items():
                    df_new = df_new.withColumn(k, F.lit(v))

            df = df.unionByName(df_new, allowMissingColumns=True)

        return df

    def get_warehouses(self) -> DataFrame:
        """
        List the databricks warehouses
        """
        schema = ST.StructType([
                    ST.StructField('auto_stop_mins', ST.IntegerType(), True),
                    ST.StructField('creator_name', ST.StringType(), True),
                    ST.StructField('enable_photon', ST.BooleanType(), True),
                    ST.StructField('enable_serverless_compute', ST.BooleanType(), True),
                    ST.StructField('id', ST.StringType(), True),
                    # ST.StructField('jdbc_url', ST.StringType(), True),
                    # ST.StructField('max_num_clusters', ST.IntegerType(), True),
                    # ST.StructField('min_num_clusters', ST.IntegerType(), True),
                    # ST.StructField('num_clusters', ST.IntegerType(), True),
                    ST.StructField('name', ST.StringType(), True),
                    # ST.StructField('num_active_sessions', ST.IntegerType(), True),
                    # ST.StructField('odbc_params', ST.StructType([
                    #     ST.StructField('hostname', ST.StringType(), True),
                    #     ST.StructField('path', ST.StringType(), True),
                    #     ST.StructField('port', ST.IntegerType(), True),
                    #     ST.StructField('protocol', ST.StringType(), True)
                    #     ]), True),
                    ST.StructField('spot_instance_policy', ST.StringType(), True),
                    # ST.StructField('state', ST.StringType(), True),
                    ST.StructField('tags', ST.StructType([
                        ST.StructField('custom_tags', ST.ArrayType(
                            ST.MapType(ST.StringType(), ST.StringType())), True)
                        ]), True),
                    ST.StructField('warehouse_type', ST.StringType(), True)
                ])
        
        df_all = self.spark.createDataFrame([], schema)

        for workspace in self.account_client.workspaces.list():
            workspace_client = utils.get_workspace_client(self.account_client,
                                                workspace.deployment_name)
            add_cols = {"workspace_id": workspace.workspace_id}
            df_all = self._union(df_all, workspace_client.warehouses.list(), schema, add_cols)

        return df_all

    def get_clusters(self) -> DataFrame:
        """
        List the databricks clusters
        """

        schema = ST.StructType([
                    # ST.StructField('num_workers', ST.IntegerType(), True),
                    # ST.StructField('autoscale', ST.MapType(
                    #     ST.StringType(), ST.IntegerType()), True),
                    ST.StructField('cluster_name', ST.StringType(), True),
                    ST.StructField('spark_version', ST.StringType(), True),
                    # ST.StructField('spark_conf', ST.MapType(
                    #     ST.StringType(), ST.StringType()), True),
                    # ST.StructField('azure_attributes', ST.StructType([
                    #     ST.StructField('log_analytics_info', ST.MapType(
                    #         ST.StringType(), ST.StringType()), True),
                    #     ST.StructField('first_on_demand', ST.IntegerType(), True),
                    #     ST.StructField('availability', ST.StringType(), True),
                    #     ST.StructField('spot_bid_max_price', ST.FloatType(), True)
                    #     ]), True),
                    ST.StructField('node_type_id', ST.StringType(), True),
                    ST.StructField('driver_node_type_id', ST.StringType(), True),
                    # ST.StructField('ssh_public_keys', ST.ArrayType(ST.StringType()), True),
                    ST.StructField('custom_tags', ST.MapType(
                        ST.StringType(), ST.StringType()), True),
                    # ST.StructField('spark_env_vars', ST.MapType(
                    #     ST.StringType(), ST.StringType()), True),
                    ST.StructField('autotermination_minutes', ST.IntegerType(), True),
                    # ST.StructField('enable_elastic_disk', ST.BooleanType(), True),
                    # ST.StructField('instance_pool_id', ST.StringType(), True),
                    # ST.StructField('policy_id', ST.StringType(), True),
                    # ST.StructField('enable_local_disk_encryption', ST.BooleanType(), True),
                    # ST.StructField('driver_instance_pool_id', ST.StringType(), True),
                    # ST.StructField('workload_type', ST.StructType([
                    #     ST.StructField('clients', ST.MapType(
                    #         ST.StringType(), ST.StringType()), True)
                    #     ]), True),
                    ST.StructField('runtime_engine', ST.StringType(), True),
                    ST.StructField('data_security_mode', ST.StringType(), True),
                    ST.StructField('single_user_name', ST.StringType(), True),
                    ST.StructField('cluster_id', ST.StringType(), True),
                    # ST.StructField('cluster_source', ST.StringType(), True),
                    # ST.StructField('creator_user_name', ST.StringType(), True),
                    # ST.StructField('driver', ST.StructType([
                    #     ST.StructField('host_private_ip', ST.StringType(), True),
                    #     ST.StructField('private_ip', ST.StringType(), True),
                    #     ST.StructField('public_dns', ST.StringType(), True),
                    #     ST.StructField('node_id', ST.StringType(), True),
                    #     ST.StructField('instance_id', ST.StringType(), True),
                    #     ST.StructField('start_timestamp', ST.LongType(), True),
                    #     ]), True),
                    # ST.StructField('executors', ST.ArrayType(ST.StructType([
                    #     ST.StructField('host_private_ip', ST.StringType(), True),
                    #     ST.StructField('private_ip', ST.StringType(), True),
                    #     ST.StructField('public_dns', ST.StringType(), True),
                    #     ST.StructField('node_id', ST.StringType(), True),
                    #     ST.StructField('instance_id', ST.StringType(), True),
                    #     ST.StructField('start_timestamp', ST.LongType(), True),
                    #     ])), True),
                    ST.StructField('spark_context_id', ST.LongType(), True),
                    # ST.StructField('jdbc_port', ST.IntegerType(), True),
                    # ST.StructField('state', ST.StringType(), True),
                    # ST.StructField('state_message', ST.StringType(), True),
                    # ST.StructField('start_time', ST.LongType(), True),
                    # ST.StructField('terminated_time', ST.LongType(), True),
                    # ST.StructField('last_state_loss_time', ST.LongType(), True),
                    # ST.StructField('last_restarted_time', ST.LongType(), True),
                    ST.StructField('cluster_memory_mb', ST.IntegerType(), True),
                    ST.StructField('cluster_cores', ST.FloatType(), True),
                    ST.StructField('default_tags', ST.MapType(
                        ST.StringType(), ST.StringType()), True),
                    # ST.StructField('cluster_log_status', ST.StructType([
                    #     ST.StructField('last_attempted', ST.LongType(), True),
                    #     ST.StructField('last_exception', ST.StringType(), True)
                    #     ]), True),
                    # ST.StructField('termination_reason', ST.StructType([
                    #     ST.StructField('code', ST.StringType(), True),
                    #     ST.StructField('type', ST.StringType(), True),
                    #     ST.StructField('parameters', ST.MapType(
                    #         ST.StringType(), ST.StringType()), True),
                    #     ]), True)
                ])

        df_all = self.spark.createDataFrame([], schema)

        for workspace in self.account_client.workspaces.list():
            workspace_client = utils.get_workspace_client(self.account_client,
                                                          workspace.deployment_name)
            add_cols = {"workspace_id": workspace.workspace_id}
            df_all = self._union(df_all, workspace_client.clusters.list(), schema, add_cols)

        return df_all

    def get_cluster_privileges(self):
        """
        List all databricks cluster privileges
        """
        perms = workspace_permissions.ClusterPermissions(self.spark, self.account_client)
        return perms.get_permissions()

    def get_warehouse_privileges(self):
        """
        List all databricks warehouse privileges
        """
        perms = workspace_permissions.WarehousePermissions(self.spark, self.account_client)
        return perms.get_permissions()
    
    def get_dashboard_privileges(self):
        """
        List all databricks dashboard privileges
        """
        perms = workspace_permissions.DashboardPermissions(self.spark, self.account_client)
        return perms.get_permissions()

    def get_volume_privileges(self):
        """
        List all databricks volume privileges
        """
        perms = workspace_permissions.VolumePermissions(self.spark, self.account_client)
        return perms.get_permissions()