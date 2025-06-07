import pandas as pd
from google.cloud import bigquery
import unicodedata
import re
from fuzzywuzzy import fuzz
import os
import glob

''' This code uses the table of Scopus researchers to find their OpenAlex profiles, based on matching DOI document signatures. Then gathers OpenAlex AuthorID and position.'''

#### LOAD EXCEL FILES #####
excel_dir = "publicaciones_scopus_excel"

# Find all .xls files in the directory
excel_files = glob.glob(os.path.join(excel_dir, "*.xlsx"))

# Read each Excel file into a DataFrame and concatenate them
dfs = []
for file in excel_files:
    df_temp = pd.read_excel(file)
    dfs.append(df_temp)

df_inv = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

##### BIGQUERY INV INVESTIGATORS TABLE #####

client = bigquery.Client(project="steadfast-task-437611-f3")

# Searches works table in insyspo, then grabs the dois where we have a match in the excel sheets
doi_list = df_inv["prism:doi"].dropna().str.strip().unique().tolist()

query_works = """
SELECT
    w.id AS work_id,
    w.doi
FROM
    insyspo.publicdb_openalex_2025_03_rm.works w
WHERE w.doi IN UNNEST(@doi_list)
"""
job_config_works = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ArrayQueryParameter("doi_list", "STRING", doi_list)]
)
df_works = client.query(query_works, job_config=job_config_works).to_dataframe()

# Merge the work_id into df_inv on 'doi'. This is the OpenAlex ID identifier for the work.
df_inv = df_inv.rename(columns={"prism:doi": "doi"})
df_inv = df_inv.merge(df_works, on="doi", how="left")


# Get Authorship Info for Each Work ID
df_inv["work_id"] = pd.to_numeric(df_inv["work_id"], errors="coerce")
work_id_list = df_inv["work_id"].dropna().astype(int).tolist()

if work_id_list:
    query_authorships = """
    SELECT
        wa.work_id,
        wa.author_position,
        wa.author_id
    FROM
        insyspo.publicdb_openalex_2025_03_rm.works_authorships wa
    WHERE
        wa.work_id IN UNNEST(@work_id_list)
    """
    job_config_auth = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("work_id_list", "INT64", work_id_list)]
    )
    df_authorships = client.query(query_authorships, job_config=job_config_auth).to_dataframe()
else:
    df_authorships = pd.DataFrame()

# Merge authorship info with df_inv on work_id.
df_inv = df_inv.merge(df_authorships, on="work_id", how="left")


# Get Author Details from Authors Table (Including Alternative Names)
df_inv["author_id"] = pd.to_numeric(df_inv["author_id"], errors="coerce")
author_id_list = df_inv["author_id"].dropna().astype(int).tolist()

if author_id_list:
    # Query authors table
    query_authors = """
    SELECT
        a.id AS author_id,
        a.display_name
    FROM
        insyspo.publicdb_openalex_2024_10_rm.authors a
    WHERE
        a.id IN UNNEST(@author_id_list)
    """
    job_config_authors = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("author_id_list", "INT64", author_id_list)]
    )
    df_authors = client.query(query_authors, job_config=job_config_authors).to_dataframe()

# Merge the author details into df_inv
df_inv = df_inv.merge(df_authors, on="author_id", how="left")

# Save Results to a New Table (Safe Test Table)
table_id = "userdb_JC.investigadores_final_temp"
job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(df_inv, table_id, job_config=job_config_load).result()

print("✅ Results saved to userdb_JC.investigadores_temp (safe test).")
