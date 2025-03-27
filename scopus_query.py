from google.cloud import bigquery
import elsapy
import requests
import pandas


# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# Initialize dataframe environment
query_inv = """
SELECT *
FROM userdb_JC.investigadores_template
"""
df_main = client.query(query_inv).to_dataframe()
