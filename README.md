# data_platform_tools

The 'data_platform_tools' project was generated by using the Databricks data assest bundle.

The tools ingest complimentary data to databricks system tables to be used in dashboards for data operations using Databricks.  

## Prerequisites
data_platform_tools requires:
- A databricks workspace with Unity Catalog enabled
- An Azure Key Vault-backed secret scope named 'kv-redkic-ne-test',  
(see https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes)
- A Microsoft Entra ID service principal named  'sp-dp-databricksapi' for OAuth M2M
   - added to your Azure Databricks account
   - added to each workspace with admin permission
   - with the SPN secret stored in key vault with key 'DatabricksAPI'
   
   (*)see https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-m2m#step-3-add-the-service-principal-to-your-azure-databricks-workspace)


## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] data_platform_tools_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/dataplatformtools_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
