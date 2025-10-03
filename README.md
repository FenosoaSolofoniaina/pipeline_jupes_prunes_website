Welcome to the pipeline_jupes_prunes_website project

This is a pipeline using ETL concept

1. E like Extraction
    - We extract data from the website (https://lesjupesdeprune.com/collections/les-jupes-2)[https://lesjupesdeprune.com/collections/les-jupes-2?page=1] using the python's libraries *requests* and *beautiful soup* (thanks)
    - We load it into a (BigQuery)[https://console.cloud.google.com/bigquery] dataset (after creating a projetc of course)
2. T like Transformation
    - Using (DBT)[https://docs.getdbt.com] to formating, create new column, ...
3. L likde Load
    - Finally, we load the transformed data into another table into the bigquery project

Thanks and enjoy.