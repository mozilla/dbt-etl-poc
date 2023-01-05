import pandas as pd

from google.cloud import bigquery

def create_query(source_project):
    """Create query for a source project."""
    return f"""
        SELECT
          DATE(creation_time) AS creation_date,
          table_catalog AS project_id,
          table_schema AS dataset_id,
          table_name AS table_id,
          table_type,
          FROM `{source_project}.region-us.INFORMATION_SCHEMA.TABLES`
        ORDER BY creation_date, project_id, dataset_id, table_id, table_type
    """


def model(dbt, session):
    """Run query for each source project."""
    dbt.config(materialization="table")

    source_projects = dbt.config.get("source_projects", ["dbt-sandbox-373717"])

    data = None

    for project in source_projects:
        client = bigquery.Client(project)
        query = create_query(project)
        if data is None:
            data = client.query(query).to_dataframe()
        else:
            data = pd.concat(data, client.query(query).to_dataframe())

    return data
