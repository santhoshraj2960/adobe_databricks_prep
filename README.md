# Adobe Databricks Prep

Learning resource used
- https://app.pluralsight.com/library/courses/building-etl-pipeline-microsoft-azure-databricks/table-of-contents

The repo contains 2 main folders
- https://github.com/santhoshraj2960/adobe_databricks_prep/tree/main/notebooks/PSDemoFolder/PluralSightDemoProdSanthosh
    - This link contains the cleaned up ETL code for Yellow, Green and Fhv taxi trips. It also contains a master orchestration notebook which invokes the           yellow_taxi_etl_notebook, green_taxi_etl_notebook and fhv_taxi_etl_notebook. Used the notebooks in this folder to create a data orchestraion job from Azure         Data Factory.

- https://github.com/santhoshraj2960/adobe_databricks_prep/tree/main/notebooks/PSDemoFolder/PluralSightDemoSanthosh
    - This link has the pyspark notebooks that I used to explore Azure Databricks. Tried ETL process for Green Taxi, Yellow Taxi and Fhv taxi data (data           collected from nyc taxi website). Later tried to generate reports of KPIs to understand which "Borough" and "Taxi Type" are the most demanding ones.

# Portal homepage that lists the different components used in the project
![alt text](https://github.com/santhoshraj2960/adobe_databricks_prep/blob/main/screenshots/all_azure_resources_used.png)

# Databricks workspaces used in the project
![alt text](https://github.com/santhoshraj2960/adobe_databricks_prep/blob/main/screenshots/databricks_workspace_used.png)

# spark cluster used in the project
![alt text](https://github.com/santhoshraj2960/adobe_databricks_prep/blob/main/screenshots/cluster_used_for_taxi_project.png)

# Azure Data Factory pipeline window
![alt text](https://github.com/santhoshraj2960/adobe_databricks_prep/blob/main/screenshots/ETL_pipeline_data_factory.png)

# Azure Data Factory pipeline runs
![alt text](https://github.com/santhoshraj2960/adobe_databricks_prep/blob/main/screenshots/ETL_pipeline_run.png)
