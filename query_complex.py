import pandas as pd
import requests
from google.cloud import bigquery
import re


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

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# Initialize dataframe environment
query_inv = """
SELECT *
FROM userdb_JC.investigadores_template
"""
df_main = client.query(query_inv).to_dataframe()

candidate_dict = {}

# Create a global cache for institution IDs
ins_id_dict = {}

def search_openalex(fs_id, q_name, full_name, pais, ins):
    """Query OpenAlex API with a general query and refine"""

    email = "jcere@umich.edu"
    ins_id = None

    # Specific case: name of university in Catalan
    if ins == "universidad politécnica de catalunya":
        ins = "universitat politècnica de catalunya"
    if ins == "universidad de valencia":
        ins = "universitat de valència"
    if ins == "universidad de alcalá de henares":
        ins = "universidad de alcalá"

    # Remove text within parentheses and after commas and assign the cleaned institution name
    ins_clean = re.sub(r"\s*(\(.*?\)|,.*|/.*)", "", ins).strip()

    # In the OpenAlex API to filter on institutions we first find the institution ID and then use it in the author search.
    # We will cache the institution ID for each unique institution name to avoid redundant API calls.
    if ins_clean not in ins_id_dict:
        institution_search = f"https://api.openalex.org/autocomplete/institutions?q={ins_clean}&mailto={email}"
        try:
            response = requests.get(institution_search)
            if response.status_code != 200:
                print(f"Error fetching OpenAlex institution data for '{ins_clean}': status code {response.status_code}")
                return None, None
            data = response.json()
            if not data.get("results"):
                print(f"No institution results for '{ins_clean}'")
                return None, None
            ins_id = data["results"][0]["id"]
            ins_id_dict[ins_clean] = ins_id
        except Exception as e:
            print(f"Error fetching OpenAlex institution data for '{ins_clean}': {e}")
            return None, None
    else:
        ins_id = ins_id_dict[ins_clean]


    # functionality that dynamically builds the OpenAlex API URL based on the information if found.
    base_autocomplete_url = "https://api.openalex.org/autocomplete/authors?"
    url = f"{base_autocomplete_url}search={q_name}&mailto={email}"

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

    # Create candidates list to store in the dictionary to save multiple profiles for the same fs_id
    if data.get("meta", {}).get("count", 0) > 0:
        # Iterate over the results, now we need to filter by institution, country
        for candidate in data["results"]:

            candidate_name = candidate.get("display_name", "")
            candidate_display_name_alternatives = candidate.get("display_name_alternatives", "")

            if candidate_name == full_name:
                

            # Matching process on our institution and country
            affiliations = candidate.get("affiliations", [])
            candidate_country = candidate.get("last_known_institution", {}).get("country_code", "")
            candidate_institutions = [aff["id"] for aff in affiliations] if affiliations else []


            candidate_alex_id = candidate.get("id", "")

            # orcid, scopus
            # Safely extract only 'orcid' and 'scopus' if they exist
            other_ids = candidate.get("ids", {})
            candidate_orc_id = ", ".join(f"{k}: {v}" for k, v in other_ids.items() if k in ["orcid"])
            candidate_scopus_id = ", ".join(f"{k}: {v}" for k, v in other_ids.items() if k in ["scopus"])

            candidate_works_count = candidate.get("citation_count", "")
            candidate_cited_by_count = candidate.get("cited_by_count", "")

            # gather summary_stats for 2yr_mean_citedness, h_index, i10_index
            summary_stats = candidate.get("summary_stats", {})
            candidate_summary_stats = ", ".join(f"{k}: {v}" for k, v in summary_stats.items())

            # Process x_concepts safely by checking if the list is not empty
            x_concepts = candidate.get("x_concepts", [])
            candidate_field = x_concepts[0].get("display_name", "") if x_concepts else ""

            # Create a tuple with the candidate information
            candidate_tuple = (
                fs_id,
                candidate_name,
                candidate_display_name_alternatives,
                candidate_field,
                candidate_alex_id,
                candidate_orc_id,
                candidate_scopus_id,
                candidate_works_count,
                candidate_cited_by_count,
                candidate_summary_stats,
            )

            # Append the candidate tuple to the dictionary that uses fs_id as the key
            candidate_dict[fs_id].append(candidate_tuple)
    return None, None

# Process all researchers to build candidate_dict
for i, (fs_id, name, ap1, full_name, pais, ins) in enumerate(
    zip(df_main["ID"], df_main["Nombre"], df_main["Apellido_1"], df_main["Nombre_apellidos"], df_main["Pais"], df_main["Trabajo_institucion"])):
    query_name = f"{name} {ap1}"
    # Execute the search, which fills in the candidate_dict for fs_id
    search_openalex(fs_id, query_name, full_name, pais, ins)

# Create a DataFrame for all candidate rows after processing all researchers
candidate_rows = []
for fs_id, candidates in candidate_dict.items():
    df_candidates = pd.DataFrame(candidates, columns=[
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

if candidate_rows:
    # Concatenate all candidate DataFrames into one
    df_candidates_final = pd.concat(candidate_rows, ignore_index=True)

    # Sort the DataFrame by fs_id in ascending order
    df_candidates_final = df_candidates_final.sort_values(by="fs_id").reset_index(drop=True)
else:
    # If no candidates found, create an empty DataFrame with columns matching the expected schema
    df_candidates_final = pd.DataFrame(columns=[
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

# Merge with the original df_main based on fs_id (ID)
df_final = df_main.merge(df_candidates_final, left_on="ID", right_on="fs_id", how="left")

table_id = "userdb_JC.investigadores_alexapi_1"
job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(df_final, table_id, job_config=job_config_load).result()