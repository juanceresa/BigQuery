import pandas as pd
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# ----------------------------
# Query the scopus_table from BigQuery.
# ----------------------------
query_scopus = """
SELECT
    ID,
    Nombre,
    `Apellido 1` AS Apellido1,
    `Apellido 2` AS Apellido2,
    `Nombre y apellidos` AS NombreYApellidos,
    `Trabajo_institucion` AS TrabajoInstitucion,
    `Año beca` AS AnioBeca,
    pais,
    GS,
    Scopus_ID
FROM userdb_JC.scopus_table
"""
df_scopus = client.query(query_scopus).to_dataframe()

# ----------------------------
# Query the investigadores_alexapi_3 table.
# (This table may have multiple rows per ID since each researcher may have multiple profiles.)
# ----------------------------
query_investigadores = """
SELECT
    ID,
    candidate_alex_id,
    candidate_orc_id,
    candidate_works_count,
    candidate_cited_by_count
FROM userdb_JC.investigadores_alexapi_3
"""
df_investigadores = client.query(query_investigadores).to_dataframe()

# ----------------------------
# Aggregate the investigadores_alexapi_3 table by grouping on ID.
# We want:
#  - A list (set) of unique candidate_alex_id values per researcher.
#  - A representative candidate_orc_id (using the first one).
#  - Sum the candidate_works_count and candidate_cited_by_count.
# ----------------------------
df_investigadores_agg = df_investigadores.groupby("ID").agg({
    "candidate_alex_id": lambda x: list(set(x.dropna())),
    "candidate_orc_id": "first",
    "candidate_works_count": "sum",
    "candidate_cited_by_count": "sum"
}).reset_index()

# ----------------------------
# Merge the aggregated investigadores_alexapi_3 table with the scopus_table on "ID".
# This results in one row per researcher that includes the scopus details as well
# as a new column with the list of candidate_alex_id profiles and total counts.
# ----------------------------
df_compiled = pd.merge(df_scopus, df_investigadores_agg, on="ID", how="left")

# Display the first few rows of the resulting DataFrame.
print(df_compiled.head())
