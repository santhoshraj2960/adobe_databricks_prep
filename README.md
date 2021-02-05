# Adobe Databricks Prep

The repo contains 2 main folders
- https://github.com/santhoshraj2960/adobe_databricks_prep/tree/main/notebooks/PSDemoFolder/PluralSightDemoSanthosh
- https://github.com/santhoshraj2960/adobe_databricks_prep/tree/main/notebooks/PSDemoFolder/PluralSightDemoProdSanthosh

The first link has the python notebooks that I used to explore Azure Databricks. Tried ETL process for Green Taxi, Yellow Taxi and Fhv taxi data (data collected from nyc taxi website). Later tried to generate reports of KPIs to understand which "Borough" and "Taxi Type" are the most demanding ones.

The second link contains the cleaned up ETL code for Yellow, Green and Fhv taxi trips. It also contains a master orchestration notebook which invokes the yellow_taxi_etl_notebook, green_taxi_etl_notebook and fhv_taxi_etl_notebook. Used the notebooks in this folder to create a data orchestraion job from Azure Data Factory.
