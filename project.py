import pandas as pd
from google.cloud import bigquery
import unicodedata

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# -----------------------
# Step 1: Load Investigators Table (Original Data)
# -----------------------
query_inv = """
SELECT ID, DOI AS doi, Nombre_apellidos, Alex_ID, Author_Pos
FROM userdb_JC.investigadores
"""
df_inv = client.query(query_inv).to_dataframe()

# -----------------------
# Step 2: Find Work IDs from OpenAlex
# -----------------------
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

# -----------------------
# Step 3: Get Authorship Info for Each Work ID
# -----------------------
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

# -----------------------
# Step 4: Get Author Details from Authors Table (Including Alternative Names)
# -----------------------
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

# -----------------------
# Step 5: Compare Investigator Name to Author Names (Including Alternative Names)
# -----------------------

def normalize_name(name):
    if pd.isna(name):
        return ""

    # Convert to string, strip spaces
    name = str(name).strip().lower()
    # Replace all types of hyphens and dashes with spaces
    name = name.replace("-", " ").replace("‐", " ").replace("⁎", " ")
    # Normalize accents to basic Latin (optional)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
    return name


def match_names(row):
    inv_name = normalize_name(row["Nombre_apellidos"])
    auth_name = normalize_name(row.get("display_name", ""))
    alt_names = row.get("display_name_alternatives", "")

    # Convert alternatives into a list (assuming they are comma-separated)
    alt_list = [normalize_name(n) for n in str(alt_names).split(",")] if alt_names else []

    if inv_name == auth_name or inv_name in alt_list:
        return True
    return False

df_inv["name_match"] = df_inv.apply(match_names, axis=1)

# Keep only rows where we found a match
df_matched = df_inv[df_inv["name_match"]].copy()


# -----------------------
# Step 6: Update Alex_ID and Author_Pos Columns
# -----------------------
df_matched["Alex_ID"] = df_matched["author_id"].apply(lambda x: f"https://openalex.org/A{x}" if pd.notna(x) else None)
df_inv["Author_Pos"] = df_inv["Author_Pos"].combine_first(df_inv["author_position"])

# -----------------------
# Step 7: Preserve All Original Data and Keep Order by ID
# -----------------------
df_final = df_inv.copy()  # Start with full dataset
df_final.update(df_matched)  # Update only matched rows
df_final.sort_values("ID", inplace=True)  # Preserve order
df_final = df_final.drop_duplicates(subset=["ID"], keep="first")

# -----------------------
# Step 8: Save Results to a New Table (Safe Test Table)
# -----------------------
table_id = "userdb_JC.investigadores_temp"
job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(df_final, table_id, job_config=job_config_load).result()

print("✅ Results saved to userdb_JC.investigadores_temp (safe test).")
