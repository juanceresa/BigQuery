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
ins_id_dict = {}

def search_openalex(fs_id, name, pais, ins):
    """Query OpenAlex API with a general query and refine"""

    email = "jcere@umich.edu"
    ins_id = None

    # Remove text within parentheses and after commas
    ins = re.sub(r"\s*(\(.*?\)|,.*|/.*)", "", ins).strip()

    # In the OpenAlex API to filter on institutions we first find the institution ID and then use it in the author search.
    # We will cache the institution ID for each unique institution name to avoid redundant API calls.
    if ins not in ins_id_dict:
        institution_search = f"https://api.openalex.org/institutions?search={ins}&mailto={email}"
        try:
            response = requests.get(institution_search).json()
            ins_id = response["results"][0]["id"]
            ins_id_dict[ins] = ins_id
        except Exception as e:
            print(f"Error fetching OpenAlex institution data for '{ins}': {e}")
            return None, None


    # Build the API URL on n1, ap1, country, and institution
    url = f"https://api.openalex.org/authors?filter=last_known_institution.continent:{pais},affiliations.institution.id:{ins_id_dict[ins]}&search={name}&mailto={email}"
    try:
        response = requests.get(url).json()
    except Exception as e:
        print(f"Error fetching OpenAlex data for '{name}': {e}")
        return None, None

    # initialize the candidate list for the fs_id
    if fs_id not in candidate_dict:
        candidate_dict[fs_id] = []

    # Create candidates list to store in the dictionary to save multiple profiles for the same fs_id
    candidates = []
    if response.get("meta", {}).get("count", 0) > 0:
        # Iterate over the results, should not be many as we are filtering by institution, country, and name
        for candidate in response["results"]:
            candidate_name = candidate.get("display_name", "")
            candidate_display_name_alternatives = candidate.get("display_name_alternatives", "")
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

            candidate_field = candidate.get("x_concepts", [{}])[0].get("display_name", "")

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


for i, (fs_id, name, ap1, pais, ins) in enumerate(
    zip(df_main["ID"], df_main["Nombre"], df_main["Apellido1"], df_main["Pais"], df_main["Institucion"])):
    query_name = f"{name} {ap1}"

    # Execute the search, which fills in the candidate_dict for fs_id
    search_openalex(fs_id, query_name, pais, ins)

    # List to hold all candidate rows as DataFrames
    candidate_rows = []

    # Create a DataFrame for each fs_id's candidates
    # (key, value) pair where key is fs_id and value is a list of candidates
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

        # Append the candidate DataFrame to the list
        candidate_rows.append(df_candidates)

    # Concatenate all candidate DataFrames into one
    df_candidates_final = pd.concat(candidate_rows, ignore_index=True)

    # Sort the DataFrame by fs_id in ascending order
    df_candidates_final = df_candidates_final.sort_values(by="fs_id").reset_index(drop=True)

    # Merge with the original df_main based on fs_id (ID)
    df_final = df_main.merge(df_candidates_final, left_on="ID", right_on="fs_id", how="left")

    table_id = "userdb_JC.investigadores_alexapi_1"
    job_config_load = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_final, table_id, job_config=job_config_load).result()
