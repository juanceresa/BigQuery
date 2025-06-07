import pandas as pd
import requests
from google.cloud import bigquery
from fuzzywuzzy import fuzz
import re
import unicodedata
import time

''' Second Script that attempts to match misisng investigators to authors in OpenAlex database through API calls. The script searches using the OpenAlex API to find the missing profiles. '''

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# -----------------------
# Part 1: API-based Alex_ID Lookup for Missing Records
# -----------------------

# Load Investigators with Missing Alex_ID from your main table
query_missing = """
SELECT *
FROM userdb_JC.investigadores
WHERE Alex_id IS NULL
"""
df_missing = client.query(query_missing).to_dataframe()

# Create a normalized version of Nombre_apellidos for fuzzy matching
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
    # Normalize Unicode characters (remove accents)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
    return name

df_missing["normalized_name"] = df_missing["Nombre_apellidos"].apply(normalize_name)

def search_openalex(name, threshold=90):
    """Query OpenAlex API for the best author match by name using fuzzy matching.
       Only returns the match if the fuzzy score is at or above the threshold.
       Uses the first candidate only."""
    email = "jcere@umich.edu"
    # Build the API URL (do not further normalize the name for the URL)
    url = f"https://api.openalex.org/authors?filter=display_name.search:{name.replace(' ', '%20')}&mailto={email}"
    try:
        response = requests.get(url).json()
    except Exception as e:
        print(f"Error fetching OpenAlex data for '{name}': {e}")
        return None, None
    if response.get("meta", {}).get("count", 0) > 0:
        first_candidate = response["results"][0]
        candidate_name = first_candidate.get("display_name", "")
        # Use normalization for fuzzy matching comparisons
        score = fuzz.token_set_ratio(normalize_name(name), normalize_name(candidate_name))
        if score >= threshold:
            return first_candidate["id"], first_candidate["display_name"]
    return None, None

# Use a loop to process API calls so we can log progress every 1000 calls.
api_results = []
for i, norm_name in enumerate(df_missing["normalized_name"]):
    result = search_openalex(norm_name)
    api_results.append(result)
    if (i + 1) % 1000 == 0:
        print(f"Processed {i + 1} API calls...")
    # Optional: a short sleep to avoid hammering the API too hard.
    time.sleep(0.1)

# Unzip the results and assign to df_missing
if api_results:
    df_missing["Alex_id"], df_missing["matched_name"] = zip(*api_results)
else:
    df_missing["Alex_id"], df_missing["matched_name"] = (None, None)

# -----------------------
# Merge the API results back into the full table (preserving all columns)
# -----------------------

# Load the full investigators table (all columns)
df_full = client.query("SELECT * FROM userdb_JC.investigadores").to_dataframe()

# Merge the new Alex_id values (from the API search) into df_full by unique ID
df_full = df_full.merge(df_missing[["ID", "Alex_id"]], on="ID", how="left", suffixes=("", "_new"))

# Update only where the original Alex_id is missing (combine_first takes the first non-null value)
df_full["Alex_id"] = df_full["Alex_id"].combine_first(df_full["Alex_id_new"])

# Drop the temporary column
df_full.drop(columns=["Alex_id_new"], inplace=True)

# -----------------------
# Part 2: DOI-Based Author Position Lookup
# -----------------------

# Load Investigators Table again (to preserve structure for DOI-based matching)
query_inv = """
SELECT ID, DOI AS doi, Nombre_apellidos, Alex_id, Author_pos
FROM userdb_JC.investigadores
"""
df_inv = client.query(query_inv).to_dataframe()

# Find Work IDs from OpenAlex using DOIs
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

# Compute fuzzy matching scores between investigator name and candidate author names

def fuzzy_match_score(inv_name, auth_name, alt_names):
    # Compute primary fuzzy score
    score_primary = fuzz.token_set_ratio(inv_name, auth_name)
    # Process alternative names (if any, assuming comma-separated)
    alt_list = [normalize_name(n) for n in str(alt_names).split(",")] if alt_names else []
    score_alts = [fuzz.token_set_ratio(inv_name, alt) for alt in alt_list] if alt_list else [0]
    return max(score_primary, max(score_alts))

# Precompute normalized investigator name
df_inv["normalized_inv_name"] = df_inv["Nombre_apellidos"].apply(normalize_name)

# Compute fuzzy match score for each candidate row (using display_name fields later merged from authors table)
df_inv["fuzzy_score"] = df_inv.apply(
    lambda row: fuzzy_match_score(
        row["normalized_inv_name"],
        normalize_name(row.get("display_name", "")),
        row.get("display_name_alternatives", "")
    ),
    axis=1
)

# For each investigator (by unique ID), select the candidate with the highest fuzzy score
df_best = df_inv.loc[df_inv.groupby("ID")["fuzzy_score"].idxmax()].copy()

# Set a threshold for a good match (e.g., 90). If below threshold, we clear candidate data.
threshold = 90
df_best.loc[df_best["fuzzy_score"] < threshold, ["author_id", "author_position", "display_name", "display_name_alternatives"]] = None

# Update Author_Pos using the best match candidate's author_position
df_best["Author_pos"] = df_best["author_position"]

# -----------------------``
# Merge the Author_Pos information from the DOI lookup into the full table
# -----------------------

# Merge df_best (with Author_Pos) back into df_full on unique ID
df_full = df_full.merge(df_best[["ID", "Author_pos"]], on="ID", how="left", suffixes=("", "_new"))
df_full["Author_pos"] = df_full["Author_pos"].combine_first(df_full["Author_pos_new"])
df_full.drop(columns=["Author_pos_new"], inplace=True)

# Ensure the final table is sorted by ID (preserving original order)
df_full.sort_values("ID", inplace=True)

# -----------------------
# Save the complete, updated table back to BigQuery
# -----------------------
table_id = "userdb_JC.investigadores_temp2"
job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(df_full, table_id, job_config=job_config_load).result()

print("✅ Results saved to userdb_JC.investigadores_temp2 (safe test).")
