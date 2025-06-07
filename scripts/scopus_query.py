#!/usr/bin/env python3
import os
import re
import shutil
import json
import requests
import pandas as pd
from pandas import json_normalize
from google.cloud import bigquery
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor
from elsapy.elssearch import ElsSearch
from unidecode import unidecode

''' This code queries the Elsevier API for author information based on a list of researchers '''

# Initialize Elsevier API client
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json')
with open(config_path) as con_file:
    config = json.load(con_file)
client = ElsClient(config['apikey'])


def clear_local_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    print(f"Cleared and recreated local directory: {path}")

def clean_query_value(text):
    """
    Remove any parentheses and the text within them.
    For example: "juan carlos (joan)" becomes "juan carlos"
    """
    return re.sub(r'\s*\(.*?\)', '', text).strip()

def clean_affiliation(ins):
    """
    Remove text in parentheses (and optionally anything after a comma)
    from the affiliation string.
    """
    cleaned = re.sub(r'\(.*?\)', '', ins)
    if ',' in cleaned:
        cleaned = cleaned.split(',')[0]
    return cleaned.strip()

def search_for_author(client, first, last1, last2, ins):
    # Clean the input strings
    first_clean = clean_query_value(first)
    last_clean = clean_query_value(f"{last1} {last2}")

    # Primary search: use the cleaned first and last names.
    author_search_str = f'authlast({last_clean}) AND authfirst({first_clean})'
    print("Primary search with:", author_search_str)
    auth_srch = ElsSearch(author_search_str, 'author')
    auth_srch.execute(client)
    if auth_srch.results:
        author_id = auth_srch.results[0].get('dc:identifier', '').split(':')[-1]
        if author_id:
            return author_id

    print(f"Primary search failed for {first} {last1} {last2}. Trying with affiliation...")
    # Secondary search: include cleaned affiliation (wrapped in quotes)
    affil_clean = clean_affiliation(ins)
    author_search_str2 = f'authlast({last_clean}) AND authfirst({first_clean}) AND AFFIL("{affil_clean}")'
    print("Secondary search with:", author_search_str2)
    auth_srch2 = ElsSearch(author_search_str2, 'author')
    auth_srch2.execute(client)
    if auth_srch2.results:
        author_id = auth_srch2.results[0].get('dc:identifier', '').split(':')[-1]
        if author_id:
            return author_id

    print(f"Author search failed for {first} {last1} {last2}.")
    return None

def get_author_data(client, author_id):
    my_auth = ElsAuthor(uri=f'https://api.elsevier.com/content/author/author_id/{author_id}')
    if my_auth.read(client):
        print("Retrieved author:", my_auth.full_name)
        df = json_normalize(my_auth.__dict__)
        # Define columns to extract
        selected_columns = [
            "_data.coredata.dc:identifier",
            "_data.coredata.prism:url",
            "_data.author-profile.preferred-name.given-name",
            "_data.author-profile.preferred-name.surname",
            "_data.coredata.document-count",
            "_data.coredata.cited-by-count",
            "_data.coredata.citation-count",
            "_data.author-profile.publication-range.@start",
            "_data.author-profile.affiliation-current.affiliation.ip-doc.afdispname",
        ]
        row = {}
        for col in selected_columns:
            row[col] = df[col].iloc[0] if col in df.columns else None

        # Attempt to retrieve the documents; note that this may require elevated permissions.
        try:
            if my_auth.read_docs(client):
                row['doc_count_retrieved'] = len(my_auth._doc_list) if my_auth._doc_list else 0
            else:
                row['doc_count_retrieved'] = 0
        except Exception as e:
            row['doc_count_retrieved'] = None
            row['doc_error'] = str(e)
        return row
    else:
        print("Failed to read author data for", author_id)
        return None

# Initialize BigQuery client
big_query = bigquery.Client(project="steadfast-task-437611-f3")

# Clear local directory for results
client.local_dir = "scopus_q_results"
clear_local_dir(client.local_dir)

# Query BigQuery for the list of researchers
query_inv = """
SELECT *
FROM userdb_JC.investigadores_template
"""
df_main = big_query.query(query_inv).to_dataframe()
print("BigQuery returned", df_main.shape[0], "rows.")

all_authors_data = []

# Process each researcher
for i, (fs_id, name, ap1, ap2, full_name, pais, ins, year) in enumerate(
    zip(df_main["ID"], df_main["Nombre"], df_main["Apellido_1"], df_main["Apellido_2"],
        df_main["Nombre_apellidos"], df_main["Pais"], df_main["Trabajo_institucion"], df_main["Ano_beca"])):
    print(f"[{i}] Processing researcher {fs_id}: {name} {ap1} {ap2}")
    try:
        author_id = search_for_author(client, name, ap1, ap2, ins)
        print(f"[{i}] Search complete, author_id: {author_id}")
        if author_id:
            author_data = get_author_data(client, author_id)
            print(f"[{i}] Retrieved data for author_id: {author_id}")
            if author_data:
                # Optionally add extra fields from BigQuery row
                author_data['fs_id'] = fs_id
                author_data['full_name'] = full_name
                author_data['pais'] = pais
                author_data['ins'] = ins
                author_data['year'] = year
                all_authors_data.append(author_data)
                print(f"[{i}] Author data appended. Total authors so far: {len(all_authors_data)}")
        else:
            print(f"[{i}] Could not find author ID for {name} {ap1} {ap2}")
    except Exception as ex:
        print(f"[{i}] Exception occurred: {ex}")

print("Total authors collected:", len(all_authors_data))
if all_authors_data:
    df_all = pd.DataFrame(all_authors_data)
    output_csv = os.path.join("scopus_q_results", "all_authors.csv")
    df_all.to_csv(output_csv, index=False)
    print(f"Saved data for {len(all_authors_data)} authors to {output_csv}")
else:
    print("No author data was collected.")
