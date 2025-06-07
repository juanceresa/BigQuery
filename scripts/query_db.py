import pandas as pd
from google.cloud import bigquery
import unicodedata
import re
from fuzzywuzzy import fuzz

'''
Project:
Investigator Matching. Beginning Script that attempts to match investigators to authors in OpenAlex database hosted in BitQuery.
The script will load the investigators table, find the work IDs from OpenAlex, get authorship
'''

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")


# Load Investigators Table (Original Data)
query_inv = """
SELECT ID, DOI AS doi, Nombre_apellidos, Alex_ID, Author_Pos
FROM userdb_JC.investigadores
"""
df_inv = client.query(query_inv).to_dataframe()


# Find Work IDs from OpenAlex
query_works = """
SELECT
    w.id AS work_id,
    w.doi
FROM
    insyspo.publicdb_openalex_2024_10_rm.works w
WHERE w.doi IN UNNEST(@doi_list)
"""
doi_list = df_inv["doi"].dropna().unique().tolist()
job_config_works = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ArrayQueryParameter("doi_list", "STRING", doi_list)]
)
df_works = client.query(query_works, job_config=job_config_works).to_dataframe()

# Merge the work_id into df_inv on 'doi'
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
        insyspo.publicdb_openalex_2024_10_rm.works_authorships wa
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

    # Query display_name_alternatives table
    query_display_name_alternatives = """
    SELECT
        dna.author_id,
        STRING_AGG(dna.display_name_alternative, ', ') AS display_name_alternatives
    FROM
        insyspo.publicdb_openalex_2024_10_rm.authors_display_name_alternatives dna
    WHERE
        dna.author_id IN UNNEST(@author_id_list)
    GROUP BY dna.author_id
    """
    job_config_dna = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("author_id_list", "INT64", author_id_list)]
    )
    df_display_name_alternatives = client.query(query_display_name_alternatives, job_config=job_config_dna).to_dataframe()

    # Merge alternative display names into authors DataFrame
    df_authors = df_authors.merge(df_display_name_alternatives, on="author_id", how="left")
else:
    df_authors = pd.DataFrame()

# Merge the author details into df_inv
df_inv = df_inv.merge(df_authors, on="author_id", how="left")


# Compare Investigator Name to Author Names (Including Alternative Names)
def normalize_name(name):
    if pd.isna(name):
        return ""
    # Convert to string, strip, lower case
    name = str(name).strip().lower()
    # Replace various hyphens/dashes with a space
    name = name.replace("-", " ").replace("‐", " ").replace("⁎", " ")
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    # Collapse multiple spaces into one
    name = re.sub(r'\s+', ' ', name)
    # Normalize Unicode characters (e.g., accents)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
    return name

# Function to compute fuzzy matching score
def fuzzy_match_score(inv_name, auth_name, alt_names):
    # Compute primary fuzzy score
    score_primary = fuzz.token_set_ratio(inv_name, auth_name)
    # Process alternative names (if any)
    alt_list = [normalize_name(n) for n in str(alt_names).split(",")] if alt_names else []
    score_alts = [fuzz.token_set_ratio(inv_name, alt) for alt in alt_list] if alt_list else [0]
    return max(score_primary, max(score_alts))

# --- After merging df_inv with works, authorships, and authors ---
# (Assuming df_inv now contains columns: ID, Nombre_apellidos, work_id, author_id,
# author_position, display_name, display_name_alternatives, etc.)

# Precompute a normalized investigator name
df_inv["normalized_inv_name"] = df_inv["Nombre_apellidos"].apply(normalize_name)

# Compute fuzzy match score for each candidate row
df_inv["fuzzy_score"] = df_inv.apply(
    lambda row: fuzzy_match_score(
        row["normalized_inv_name"],
        normalize_name(row.get("display_name", "")),
        row.get("display_name_alternatives", "")
    ),
    axis=1
)

# For each investigator (using the unique ID), select the candidate with the highest fuzzy score
df_best = df_inv.loc[df_inv.groupby("ID")["fuzzy_score"].idxmax()].copy()

# Set a threshold for a good match (e.g., 90)
threshold = 90
# If the best score for an investigator is below the threshold, treat it as no match
df_best.loc[df_best["fuzzy_score"] < threshold, ["author_id", "author_position", "display_name", "display_name_alternatives"]] = None

# Update Alex_ID and Author_Pos using the best match candidate
df_best["Alex_ID"] = df_best["author_id"].apply(lambda x: f"https://openalex.org/A{int(x)}" if pd.notna(x) else None)
df_best["Author_Pos"] = df_best["author_position"]

# If needed, update your original investigators table with the best matches
# Here we sort and drop duplicates by the investigator's ID
df_final = df_best.sort_values("ID").drop_duplicates(subset=["ID"], keep="first")


# Save Results to a New Table (Safe Test Table)
table_id = "userdb_JC.investigadores_temp"
job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(df_final, table_id, job_config=job_config_load).result()

print("✅ Results saved to userdb_JC.investigadores_temp (safe test).")
