import pandas as pd
import requests
from google.cloud import bigquery
import os
import unicodedata

# Initialize BigQuery client
client = bigquery.Client(project="steadfast-task-437611-f3")

# Dictionary to hold works for each researcher: key = fs_id, value = list of tuples
# Each tuple now contains (title, doi, id, publication_year, type, cited_by_count)
candidate_works_dict = {}

# Retrieve the investigators data from BigQuery into a DataFrame
query_inv = """
SELECT *
FROM userdb_JC.investigadores_alexapi_3
"""
df_main = client.query(query_inv).to_dataframe()

def return_docs_openalex(fs_id, alex_id, q_name):
    """Query OpenAlex API to return documents for a researcher."""
    email = "jcere@umich.edu"

    if not alex_id:
        print(f"No OpenAlex id provided for '{q_name}'. Skipping.")
        return None, None

    # Convert URL from the standard web profile to the API endpoint.
    # Example: "https://openalex.org/A5074012726" becomes
    # "https://api.openalex.org/authors/A5074012726"
    alex_id = alex_id.replace("https://openalex.org/", "https://api.openalex.org/authors/")
    # Append the mailto parameter to the query string.
    url = f"{alex_id}&mailto={email}"

    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching OpenAlex id data for '{q_name}': status code {response.status_code}")
            return None, None
        data = response.json()
        author_works_url = data["works_api_url"]
    except Exception as e:
        print(f"Error fetching OpenAlex profile data for '{q_name}': {e}")
        return None, None

    # Remove extra quotes if they are present in the URL.
    url = author_works_url.split('"')[1] if '"' in author_works_url else author_works_url

    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching OpenAlex works data for '{q_name}': status code {response.status_code}")
            return None, None
        data = response.json()
    except Exception as e:
        print(f"Error fetching OpenAlex works data for '{q_name}': {e}")
        return None, None

    if data.get("meta", {}).get("count", 0) > 0:
        results = data["results"]
        for work in results:
            work_id = work.get("id", "")
            doi = work.get("doi", "")
            title = work.get("title", "")
            pub_year = work.get("publication_year", "")
            type_work = work.get("type", "")
            cited_by_count = work.get("cited_by_count", 0)

            # Optionally, add a debug print here:
            # print(f"Work: {title}, cited_by_count: {cited_by_count}")

            work_tuple = (title, doi, work_id, pub_year, type_work, cited_by_count)
            candidate_works_dict[fs_id].append(work_tuple)
    else:
        print(f"No works found for '{q_name}' in OpenAlex.")
        return None, None

# ----------------------------
# Setup output directory and clear any existing files.
# ----------------------------
output_dir = "researcher_excels"
os.makedirs(output_dir, exist_ok=True)
for file in os.listdir(output_dir):
    file_path = os.path.join(output_dir, file)
    if os.path.isfile(file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")

# ----------------------------
# Process each researcher (grouped by fs_id) in sequence.
# ----------------------------
for fs_id, group in df_main.groupby("fs_id"):
    candidate_name = group["candidate_name"].iloc[0]
    candidate_works_dict[fs_id] = []

    # For each profile (row) for this researcher, call the OpenAlex API.
    for _, row in group.iterrows():
        alex_id = row["candidate_alex_id"]
        return_docs_openalex(fs_id, alex_id, candidate_name)

    # Create a safe filename using candidate name and fs_id.
    file_name = f"{candidate_name}_{fs_id}.xlsx"
    file_name = file_name.replace("/", "_").replace("\\", "_").strip()
    file_path = os.path.join(output_dir, file_name)

    if candidate_works_dict[fs_id]:
        df_works = pd.DataFrame(candidate_works_dict[fs_id],
            columns=["title", "doi", "id", "publication_year", "type", "cited_by_count"])
    else:
        df_works = pd.DataFrame(columns=["title", "doi", "id", "publication_year", "type", "cited_by_count"])

    # Write the DataFrame to an Excel file.
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        # Write starting from row 3 (startrow=2) to leave room for the header.
        df_works.to_excel(writer, sheet_name="Works", startrow=2, index=False)
        workbook = writer.book
        worksheet = writer.sheets["Works"]
        header_text = f"Investigator: {candidate_name} | fs_id: {fs_id}"
        worksheet.write("A1", header_text)

    print(f"Created master Excel file for {candidate_name} (fs_id: {fs_id}): {file_path}")
