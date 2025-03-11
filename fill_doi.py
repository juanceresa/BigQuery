import pandas as pd
import requests
from google.cloud import bigquery
from fuzzywuzzy import fuzz
import re
import unicodedata

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# Load Investigators Table (original rows; each row should be unique per investigator)
query_inv = "SELECT * FROM userdb_JC.investigadores_temp10"
df_inv = client.query(query_inv).to_dataframe()

# -----------------------
# Step 1: Merge extra data
# -----------------------

# Find Work IDs from OpenAlex
query_works = """
SELECT w.id AS work_id, w.doi
FROM insyspo.publicdb_openalex_2024_10_rm.works w
WHERE w.doi IN UNNEST(@doi_list)
"""
doi_list = df_inv["doi"].dropna().unique().tolist()
job_config_works = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ArrayQueryParameter("doi_list", "STRING", doi_list)]
)
df_works = client.query(query_works, job_config=job_config_works).to_dataframe()

# LEFT JOIN so that all original rows remain
df_inv = df_inv.merge(df_works, on="doi", how="left")

# Get Authorship Info for Each Work ID
df_inv["work_id"] = pd.to_numeric(df_inv["work_id"], errors="coerce").astype("Int64")
work_id_list = df_inv["work_id"].dropna().astype(int).tolist()
if work_id_list:
    query_authorships = """
    SELECT wa.work_id, wa.author_position, wa.author_id
    FROM insyspo.publicdb_openalex_2024_10_rm.works_authorships wa
    WHERE wa.work_id IN UNNEST(@work_id_list)
    """
    job_config_auth = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("work_id_list", "INT64", work_id_list)]
    )
    df_authorships = client.query(query_authorships, job_config=job_config_auth).to_dataframe()
else:
    df_authorships = pd.DataFrame()

# Merge authorship info (LEFT JOIN)
df_inv = df_inv.merge(df_authorships, on="work_id", how="left")

# Get Author Details from Authors Table
df_inv["author_id"] = pd.to_numeric(df_inv["author_id"], errors="coerce").astype("Int64")
author_id_list = df_inv["author_id"].dropna().astype(int).tolist()
if author_id_list:
    query_authors = """
    SELECT a.id AS author_id, a.display_name
    FROM insyspo.publicdb_openalex_2024_10_rm.authors a
    WHERE a.id IN UNNEST(@author_id_list)
    """
    job_config_authors = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("author_id_list", "INT64", author_id_list)]
    )
    df_authors = client.query(query_authors, job_config=job_config_authors).to_dataframe()
else:
    df_authors = pd.DataFrame()

# Merge author details (LEFT JOIN)
df_inv = df_inv.merge(df_authors, on="author_id", how="left")

# -----------------------
# Step 2: Fuzzy matching
# -----------------------

# Define a function to normalize names
def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = re.sub(r'[^\w\s]', '', name)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
    return name

# Define a function to compute fuzzy score
def fuzzy_match_score(inv_name, auth_name):
    return fuzz.token_set_ratio(inv_name, auth_name)

# Create a normalized version of the investigator's name
df_inv["normalized_inv_name"] = df_inv["Nombre_apellidos"].apply(normalize_name)

# Compute fuzzy score comparing investigator name with author display_name
df_inv["fuzzy_score"] = df_inv.apply(
    lambda row: fuzzy_match_score(row["normalized_inv_name"], normalize_name(row.get("display_name", ""))),
    axis=1
)

# For rows where the fuzzy_score is below threshold, set match-related values to None
threshold = 90
df_inv.loc[df_inv["fuzzy_score"] < threshold, ["author_id", "author_position", "display_name"]] = None

# Create a new column "Author_order" that copies the best available author_position value
df_inv["Author_order"] = df_inv["author_position"]

# -----------------------
# Step 3: Map Best Match and Overwrite
# -----------------------

# For each investigator (by ID), choose the row with the highest fuzzy_score.
# This produces one best-match row per investigator.
df_best = df_inv.loc[df_inv.groupby("ID")["fuzzy_score"].idxmax()]

# Build mappings from ID to best matching author_id and Author_order
author_order_mapping = df_best.set_index("ID")["Author_order"].to_dict()
author_id_mapping = df_best.set_index("ID")["author_id"].to_dict()

# Update the original DataFrame with the best match values using the mapping.
# This effectively overwrites rows with the best match without creating new rows.
df_inv["Author_order"] = df_inv["ID"].map(author_order_mapping)
df_inv["author_id"] = df_inv["ID"].map(author_id_mapping)

# -----------------------
# Step 4: Deduplicate and Cleanup
# -----------------------

# Group by the unique investigator ID to collapse any duplicates resulting from the joins.
# Here we take the first non-null value for each column.
df_dedup = df_inv.groupby("ID", as_index=False).first()

# Drop extra columns used for matching that are not needed in the final output.
columns_to_drop = ["work_id", "display_name", "normalized_inv_name", "fuzzy_score"]
df_final = df_dedup.drop(columns=columns_to_drop, errors="ignore")

# Reorder columns to match the original table structure.
# (Assuming original columns are: Nombre, Apellido_1, Apellido_2, Nombre_apellidos,
#  Trabajo_institucion, Ano_beca, Pais, ID, GS, doi, Author_order, Alex_id, author_id)
desired_order = ["Nombre", "Apellido_1", "Apellido_2", "Nombre_apellidos",
                 "Trabajo_institucion", "Ano_beca", "Pais", "ID", "GS", "doi",
                 "Author_order", "Alex_id", "author_id"]
df_final = df_final.reindex(columns=desired_order)

# Sort the final DataFrame by ID (numeric sort)
df_final["ID"] = pd.to_numeric(df_final["ID"], errors="coerce")
df_final = df_final.sort_values("ID")

# -----------------------
# Step 5: Save to BigQuery
# -----------------------

destination_table = "userdb_JC.investigadores_temp10"
df_final.to_gbq(destination_table, project_id="steadfast-task-437611-f3", if_exists="replace")

print(f"✅ Table {destination_table} has been successfully created in BigQuery.")
