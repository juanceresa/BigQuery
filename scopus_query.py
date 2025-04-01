from google.cloud import bigquery
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import json
from unidecode import  unidecode
import os
import shutil
import json
import pandas as pd
from pandas import json_normalize
import requests


researchers_list = []

def clear_local_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    print(f"Cleared and recreated local directory: {path}")

def read_author(file_path, client, author_id):
    my_auth = ElsAuthor(uri=f'https://api.elsevier.com/content/author/author_id/{author_id}')

    # # Save the original exec_request method
    # orig_exec_request = client.exec_request

    # def new_exec_request(url, *args, **kwargs):
    #     response = orig_exec_request(url, *args, **kwargs)
    #     print("Raw API response for URL:", url)
    #     print(response)
    #     return response

    # # Override the client's exec_request method with our new function
    # client.exec_request = new_exec_request

    if my_auth.read(client):
        print("Author full name:", my_auth.full_name)
        df = json_normalize(my_auth.__dict__)
        selected_columns = [
            "_data.author-profile.preferred-name.given-name",
            "_data.author-profile.preferred-name.surname",
            "_doc_list",
            "_data.coredata.document-count",
            "_data.coredata.cited-by-count",
            "_data.coredata.citation-count",
            "_data.author-profile.publication-range.@start",
            "_data.author-profile.affiliation-current.affiliation.ip-doc.afdispname",
        ]
        df_subset = df[selected_columns]
        df_subset.to_csv(file_path, index=False)

        try:
            if my_auth.read_docs(client):
                if my_auth._doc_list and len(my_auth._doc_list) > 0:
                    print("Number of documents retrieved:", len(my_auth._doc_list))
                    my_auth.write_docs()
                else:
                    print("The document list is empty after read_docs.")
            else:
                print("Failed to retrieve documents. Check your API key, inst token, and permissions.")
        except Exception as e:
            print("An error occurred while reading documents:", e)


def author_search(file_path, client, first, last1, last2, ins):

    author_search_str = f'authlast({last1} {last2}) AND authfirst({first})'
    auth_srch = ElsSearch(author_search_str, 'author')
    auth_srch.execute(client)

    # SEARCH 1
    if auth_srch.results:
        author_id = auth_srch.results[0].get('dc:identifier', '').split(':')[-1]
        if author_id:
            read_author(file_path, client, author_id)
        else:
            #SEARCH 2
            print(f"Author ID for {first} {last1} {last2} not found in results.")
            print(f"Searching for {first} {last1} {last2} with affiliation {ins}...")

            author_search_str2 = f'authlast({last1}) AND authfirst({first}) AND AFFIL({ins})'
            auth_srch2 = ElsSearch(author_search_str2, 'author')
            auth_srch2.execute(client)

            if auth_srch2.results:
                author_id = auth_srch2.results[0].get('dc:identifier', '').split(':')[-1]
            if author_id:
               read_author(file_path, client, author_id)

            else:
                print(f"Author ID for {first} {last1} {last2} search2 failed.")
                print(f"Searching for {first} {last1} to determine if one profile exists...")

                # SEARCH 3
                author_search_str3 = f'authlast({last1}) AND authfirst({first})'
                auth_srch3 = ElsSearch(author_search_str3, 'author')
                auth_srch3.execute(client)

                if len(auth_srch3.results) == 1:
                    author_id = auth_srch3.results[0].get('dc:identifier', '').split(':')[-1]
                    if author_id:
                        read_author(file_path, client, author_id)


# Initialize BigQuery client
big_query = bigquery.Client(project="steadfast-task-437611-f3")

### Initialize ELSAPI client
con_file = open("config.json")
config = json.load(con_file)
con_file.close()
client = ElsClient(config['apikey'])
client.local_dir = "scopus_q_results"
clear_local_dir(client.local_dir)

folder_path = "scopus_q_results"

# Initialize BigQuery dataframe environment
query_inv = """
SELECT *
FROM userdb_JC.investigadores_template
"""
df_main = big_query.query(query_inv).to_dataframe()

for i, (fs_id, name, ap1, ap2, full_name, pais, ins, year) in enumerate(
    zip(df_main["ID"], df_main["Nombre"], df_main["Apellido_1"], df_main["Apellido_2"], df_main["Nombre_apellidos"], df_main["Pais"], df_main["Trabajo_institucion"], df_main["Ano_beca"])):
        filename = f"researcher_{fs_id}.csv"
        file_path = os.path.join(folder_path, filename)
        author_search(file_path, client, name, ap1, ap2, ins)
