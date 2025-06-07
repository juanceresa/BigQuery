import pandas as pd
import requests
from google.cloud import bigquery
import re
import json
import os
import unicodedata

'''
1. Buscar a AlexID con nombre 1 y Apellido 1
2. Especializar en el país y nombre de institucion
3. Revolver estos ids en un tabla nueva de personas (tabla_alex_ids)
    1.  Significa el FullBrightID, y todos los Alex id que encuentro, numero de citas, campo de investigación


1. Tabla nueva, FullBrightID, Alex_id, suma de Alex_id (n_alex_id), campos de investigación, numero de citas
    1. Si el suma de Alex_id es 1, ponga verificado
    2. Si el campo de investigación por cada ID es lo mismo, ponga verificación
    3. las otros ( ponga bandera para resolver )
    4. Entonces tenemos un Alex_ids verificados y no verificados
'''

def normalize_name(name):
    # Convert to lowercase and strip whitespace
    norm = name.strip().lower()
    # Remove accents: decompose Unicode characters and filter out combining marks
    norm = unicodedata.normalize('NFKD', norm)
    norm = "".join(ch for ch in norm if not unicodedata.combining(ch))
    # Replace hyphen-like characters with a space
    norm = re.sub(r"[-‐]", " ", norm)
    # Remove punctuation but keep alphanumeric characters and whitespace
    norm = "".join(ch for ch in norm if ch.isalnum() or ch.isspace())
    # Collapse multiple spaces into one
    norm = re.sub(r"\s+", " ", norm)
    return norm

def bag_of_words(candidate_tokens, full_name_tokens, candidate_name, candidate_alex_id, reject_candidate=False):
    if candidate_alex_id in reject_dict:
        return True
    # We check in a bag-of-words method on our profile
    # A profile could omit one of our last names we have in our full name, but we will never see a name we don't expect
    for i, token in enumerate(candidate_tokens):
        # If candidate has more tokens than full_name, then this token is extra.
        if i >= len(full_name_tokens):
            missing_token = token
            reject_candidate = True
            break
        # the goal here is to check if the first letter in the same position of the full substring is the same
        # if it is not, then we skip this candidate
        if len(token) < 2:
            if token != full_name_tokens[i][0]:
                print(f"EXPECTED INITIAL={full_name_tokens[i][0]}, GOT={token}, {full_name}, {candidate_name}\n")
                missing_token = token
                reject_candidate = True
                break
        # Check if this token appears as an exact match in any query token
        else:
            if token not in full_name_tokens:
                missing_token = token
                reject_candidate = True
                break
    if reject_candidate:
        print(f"SKIP")
        print(f"NAME: {full_name}")
        print(f"PROFILE: {candidate_name}")
        print(f"Missing Token: {missing_token}\n")
        return reject_candidate
    return False


# Global file for caching institution IDs
cache_file = "ins_id_cache.json"

# Load the institution cache from file if it exists; otherwise, start with an empty dictionary.
if os.path.exists(cache_file):
    with open(cache_file, "r") as f:
        ins_id_dict = json.load(f)
else:
    ins_id_dict = {}


# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# Initialize dataframe environment
query_inv = """
SELECT *
FROM userdb_JC.investigadores_template
"""
df_main = client.query(query_inv).to_dataframe()

# Create a global cache for candidate data
candidate_dict = {}
reject_dict = {}


def gather_data(fs_id, candidate, candidate_name, candidate_display_name_alternatives):
        candidate_alex_id = candidate.get("id", "")
        # orcid, scopus
        # Safely extract only 'orcid' and 'scopus' if they exist
        other_ids = candidate.get("ids", {})
        candidate_orc_id = ", ".join(v for k, v in other_ids.items() if k == "orcid")
        candidate_scopus_id = ", ".join(v for k, v in other_ids.items() if k == "scopus")
        candidate_works_count = candidate.get("works_count", "")
        candidate_cited_by_count = candidate.get("cited_by_count", "")
        # gather summary_stats for 2yr_mean_citedness, h_index, i10_index
        summary_stats = candidate.get("summary_stats", {})
        candidate_summary_stats = ", ".join(f"{k}: {v}" for k, v in summary_stats.items())
        # Process x_concepts safely by checking if the list is not empty
        topics = candidate.get("topics", [])
        candidate_field_name = topics[0].get("field", {}).get("display_name", "") if topics else ""

        candidate_revelance_score = None
        # Create a tuple with the candidate information
        candidate_tuple = (
            fs_id,
            candidate_name,
            candidate_display_name_alternatives,
            candidate_field_name,
            candidate_alex_id,
            candidate_orc_id,
            candidate_scopus_id,
            candidate_works_count,
            candidate_cited_by_count,
            candidate_summary_stats,
            candidate_revelance_score,
            topics
        )
        # Append the candidate tuple to the dictionary that uses fs_id as the key
        candidate_dict[fs_id].append(candidate_tuple)


def search_openalex(fs_id, q_name, full_name, pais, ins):
    """Query OpenAlex API with a general query and refine"""

    email = "jcere@umich.edu"
    ins_id = None


   # Remove text after parentheses (), commas ,, slashes /, and dashes -
    ins_clean = re.sub(r"\s*(\(.*?\)|,.*|/.*|-.*)", "", ins or "").strip()

    # In the OpenAlex API to filter on institutions we first find the institution ID and then use it in the author search.
    # We will cache the institution ID for each unique institution name to avoid redundant API calls.
    # Later in your code, when updating:
    if ins_clean not in ins_id_dict:
        institution_search = f"https://api.openalex.org/institutions?search={ins_clean}&mailto={email}"
        try:
            response = requests.get(institution_search)
            if response.status_code != 200:
                print(f"Error fetching OpenAlex institution data for '{ins_clean}': status code {response.status_code}")
                ins_id_dict[ins_clean] = "MANUAL_REQUIRED"
                with open(cache_file, "w") as f:
                    json.dump(ins_id_dict, f)
                return None, None
            data = response.json()
            if not data.get("results"):
                print(f"No institution results for '{ins_clean}'")
                ins_id_dict[ins_clean] = "MANUAL_REQUIRED"
                with open(cache_file, "w") as f:
                    json.dump(ins_id_dict, f)
                return None, None
            ins_id = data["results"][0]["id"]
            ins_id_dict[ins_clean] = ins_id
            with open(cache_file, "w") as f:
                json.dump(ins_id_dict, f)
        except Exception as e:
            print(f"Error fetching OpenAlex institution data for '{ins_clean}': {e}")
            ins_id_dict[ins_clean] = "MANUAL_REQUIRED"
            with open(cache_file, "w") as f:
                json.dump(ins_id_dict, f)
            return None, None
    else:
        ins_id = ins_id_dict[ins_clean]

    # functionality that dynamically builds the OpenAlex API URL based on the information
    url = "https://api.openalex.org/authors?"
    url = f"{url}search={q_name}&mailto={email}"

    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching OpenAlex data for '{q_name}': status code {response.status_code}")
            return None, None
        data = response.json()
    except Exception as e:
        print(f"Error fetching OpenAlex data for '{q_name}': {e}")
        return None, None

    # initialize the candidate list for the fs_id
    if fs_id not in candidate_dict:
        candidate_dict[fs_id] = []

    # Loop to create candidates list, stores multiple profiles per fs_id
    # Iterate sover the results from our first query
    if data.get("meta", {}).get("count", 0) > 0:
        results = data["results"]
        full_name_norm = normalize_name(full_name)

        ### LOOPS TO ITERATE OVER THE RESULTS ###

        ### Case 1: exact name or alternative match (iteration 1) ###
        for candidate in results:
            candidate_name = candidate.get("display_name", "")
            candidate_name_norm = normalize_name(candidate_name)
            candidate_display_name_alternatives = candidate.get("display_name_alternatives", [])
            alternatives_norm = [normalize_name(alt) for alt in candidate_display_name_alternatives]

            if candidate_name_norm == full_name_norm or any(full_name_norm == alt for alt in alternatives_norm):
                candidate_alex_id = candidate.get("id", "")
                # Skip if already added
                if not any(candidate_alex_id == ct[4] for ct in candidate_dict.get(fs_id, [])):
                    gather_data(fs_id, candidate, candidate_name, candidate_display_name_alternatives)
                    print(f"\n----- EXACT MATCH -----")
                    print(f"NAME:{full_name}")
                    print(f"PROFILE:{candidate_name}\n")

        ### Case 2: Institution match with additional fuzzy name check (iteration 2) ###
        for candidate in results:
            candidate_alex_id = candidate.get("id", "")
            candidate_name = candidate.get("display_name", "")

            full_name_tokens = normalize_name(full_name).split()
            candidate_tokens = normalize_name(candidate_name).split()

            # Skip candidate profile if already added in Case 1
            if any(candidate_alex_id == ct[4] for ct in candidate_dict.get(fs_id, [])):
                continue
            # Skip candidate if we have a token mismatch
            reject = bag_of_words(candidate_tokens, full_name_tokens, candidate_name, candidate_alex_id)
            if reject:
                if candidate_alex_id not in reject_dict:
                    reject_dict[candidate_alex_id] = True
                continue

            affiliations = candidate.get("affiliations", [])
            candidate_institutions = [
                aff.get("institution", {}).get("id", "")
                for aff in affiliations
                if aff.get("institution", {}).get("id")
            ]

            if ins_id and ins_id in candidate_institutions:
                print(f"\n----- INS MATCH -----")
                print(f"NAME:{full_name}")
                print(f"PROFILE: {candidate_name}")
                print(f"INSTITUTION: {ins}\n")
                gather_data(fs_id, candidate, candidate_name, candidate.get("display_name_alternatives", []))

        ### Case 3: topics matching ###
        for candidate in results:
            candidate_alex_id = candidate.get("id", "")
            candidate_name = candidate.get("display_name", "")

            full_name_tokens = sorted(normalize_name(full_name).split())
            candidate_tokens = sorted(normalize_name(candidate_name).split())

            # Skip candidate profile if already added in Cases 1 or 2
            if any(candidate_alex_id == ct[4] for ct in candidate_dict.get(fs_id, [])):
                continue

            # Skip candidate if we have a token mismatch
            reject = bag_of_words(candidate_tokens, full_name_tokens, candidate_name, candidate_alex_id)
            if reject:
                if reject_dict.get(candidate_alex_id, False):
                    reject_dict[candidate_alex_id] = True
                continue

            existing_candidates = candidate_dict.get(fs_id, [])
            potential_candidate_topics = candidate.get("topics", [])

            # Find the existing candidate with the highest relevance score for this fs_id
            max_revelance_exisiting = -1
            comparison_candidate = None
            for existing_candidate in existing_candidates:
                relevance = existing_candidate[10] if existing_candidate[10] is not None else 0
                if relevance > max_revelance_exisiting:
                    comparison_candidate = existing_candidate
                    max_revelance_exisiting = relevance

            # If no candidate has been added yet, skip Case 3
            if comparison_candidate is None:
                continue

            # Get the topics list from the comparison candidate
            existing_profile_topics = comparison_candidate[-1]  # topics list stored as the last element
            # Compare only the top 2 topics from each list
            for topic in existing_profile_topics[:2]:
                for topic_comparison in potential_candidate_topics[:2]:
                    # Safely get the display_name from the nested dictionaries
                    topic_display = topic.get("field", {}).get("display_name", "")
                    topic_comp_display = topic_comparison.get("field", {}).get("display_name", "")
                    if topic_display == topic_comp_display:

                        print(f"\n----- TOPIC MATCH -----")
                        print(f"EXISTING: {comparison_candidate[1]}, {comparison_candidate[2]}")
                        print(f"COMPARISON: {candidate_name}")
                        print(f"TOPIC: {topic_display}\n")

                        gather_data(fs_id, candidate, candidate_name, candidate.get("display_name_alternatives", []))
                        # Once a match is found for this candidate, no need to check further:
                        break
            continue

# Process all researchers to build candidate_dict
for i, (fs_id, name, ap1, full_name, pais, ins) in enumerate(
    zip(df_main["ID"], df_main["Nombre"], df_main["Apellido_1"], df_main["Nombre_apellidos"], df_main["Pais"], df_main["Trabajo_institucion"])):
    query_name = f"{name} {ap1}"
    # Execute the search, which fills in the candidate_dict for fs_id
    search_openalex(fs_id, query_name, full_name, pais, ins)

# Create a DataFrame for all candidate rows after processing all researchers
candidate_rows = []
for fs_id, candidates in candidate_dict.items():
    # Drops the x_concepts column from the candidate tuple, as it is not needed in the final DataFrame
    df_candidates = pd.DataFrame([t[:-2] for t in candidates], columns=[
        "fs_id",
        "candidate_name",
        "candidate_display_name_alternatives",
        "candidate_field",
        "candidate_alex_id",
        "candidate_orc_id",
        "candidate_scopus_id",
        "candidate_works_count",
        "candidate_cited_by_count",
        "candidate_summary_stats",
    ])
    candidate_rows.append(df_candidates)

# Concatenate all candidate DataFrames into one (done once)
df_candidates_final = pd.concat(candidate_rows, ignore_index=True)

# Sort the DataFrame by fs_id in ascending order
df_candidates_final = df_candidates_final.sort_values(by="fs_id").reset_index(drop=True)

# Merge with the original df_main based on fs_id (ID)
df_final = df_main.merge(df_candidates_final, left_on="ID", right_on="fs_id", how="left")

table_id = "userdb_JC.investigadores_alexapi_3"
job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(df_final, table_id, job_config=job_config_load).result()