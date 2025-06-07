# -*- coding: utf-8 -*-
# Made by : Juan Ceresa
"""
High-Level Overview:
  1. Scan the folder 'publicaciones_openalex_per_researcher' for each researcher’s spreadsheet.
  2. Detect rows flagged for first author match (unique_author_first_appearance).
  3. For each flagged author ID:
       - Fetch author profile from OpenAlex database.
       - Confirm that the researcher’s last name appears in the profile’s display name.
       - Retrieve all publications (works) for that author via OpenAlex API, handling pagination.
  4. Aggregate retrieved publications into two outputs:
       a) A metadata summary of total works and citation counts (calculated from the retrieved records).
       b) A detailed list of every publication (title, doi, year, citations).
  5. Save each researcher’s combined results to an Excel file with separate sheets for metadata and works.

Usage:
  python open_alex_document_fill.py [--clear-dir]

Options:
  --clear-dir   Clear the output directory before running, so only fresh files are generated.
                If omitted, existing output files are left in place and skipped if present.
"""

import os
import time
import shutil
import argparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import unicodedata
from zipfile import BadZipFile

# Directories and API base URL
INPUT_DIR  = "publicaciones_openalex_per_researcher"  # where input spreadsheets live
OUTPUT_DIR = "openalex_document_tables"               # where to write results
BASE_AUTHOR_URL = "https://api.openalex.org/authors/"  # endpoint for author profiles

# Set up an HTTP session with retry logic to handle transient network/SSL issues
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def fetch_json(url, params=None):
    """
    Retrieve JSON data from OpenAlex, retrying on errors.
    If an SSL handshake fails, retry once without verification.
    """
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.SSLError:
        # second attempt without SSL validation
        response = session.get(url, params=params, timeout=10, verify=False)
        response.raise_for_status()
    return response.json()


def strip_accents(text: str) -> str:
    """
    Convert accented characters to their unaccented equivalents.
    Helps match names that include special characters.
    """
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(ch for ch in normalized if unicodedata.category(ch) != 'Mn')


def get_all_works(works_api_url):
    """
    Fetch all publication records for an author by following "next" links in OpenAlex’s API.
    Collects title, DOI, year, and citation count for each work.
    """
    works = []
    url = works_api_url
    params = {"per-page": 200}
    while url:
        page = fetch_json(url, params=params)
        works.extend(page.get("results", []))
        url = page.get("meta", {}).get("next")  # next page URL if available
        time.sleep(0.1)  # avoid hitting rate limits
        params = None     # only send params on first request
    return works


def read_raw(path):
    """
    Attempt to read the file as Excel (.xlsx/.xls) or fallback to CSV.
    Returns a raw DataFrame with no header, used to detect where the header row lives.
    """
    ext = os.path.splitext(path)[1].lower()
    # Try reading as Excel with either engine
    if ext in ('.xlsx', '.xls'):
        engines = ['openpyxl'] if ext == '.xlsx' else []
        engines.append('xlrd')
        for engine in engines:
            try:
                return pd.read_excel(path, header=None, engine=engine)
            except (ValueError, BadZipFile, ImportError):
                continue
    # Fallback: try reading as CSV
    try:
        return pd.read_csv(path, header=None)
    except Exception as e:
        raise ValueError(f"Cannot read file {path}: {e}")


def read_data_with_dynamic_header(path, flag_col_name):
    """
    Detect which row contains the column names by looking for the flag column
    Then re-read the file using that row as header and strip whitespace from names.
    """
    raw = read_raw(path)
    header_row = None
    for idx, row in raw.iterrows():
        if row.astype(str).str.contains(flag_col_name, case=False, na=False).any():
            header_row = idx
            break
    if header_row is None:
        return None
    ext = os.path.splitext(path)[1].lower()
    if ext in ('.xlsx', '.xls'):
        engine = 'openpyxl' if ext == '.xlsx' else 'xlrd'
        df = pd.read_excel(path, header=header_row, engine=engine)
    else:
        df = pd.read_csv(path, header=header_row)
    df.columns = df.columns.str.strip()
    return df


def process_files():
    """
    Main routine:
      - Iterate each researcher file
      - Identify rows flagged TRUE in unique_author_first_appearance
      - Verify author matches profile in OpenAlex
      - Gather all publications and compute totals
      - Output two-sheet Excel per researcher
    """
    flag = "unique_author_first_appearance"

    for fname in os.listdir(INPUT_DIR):
        # Ignore any non-spreadsheet files
        if not fname.lower().endswith(('.xlsx', '.xls', '.csv')):
            continue

        # Extract the researcher’s base name and potential surname(s)
        slug, _ = os.path.splitext(fname)
        parts = slug.split('_')
        name_parts = parts[:-1]
        raw_surnames = name_parts[-2:] if len(name_parts) > 2 else [name_parts[-1]]
        surnames = [strip_accents(s).lower() for s in raw_surnames]

        out_path = os.path.join(OUTPUT_DIR, fname)
        if os.path.exists(out_path):
            print(f"[Skip] {fname} already exists.")
            continue  # don’t overwrite existing data

        # Load the sheet and locate the flag column header
        df = None
        try:
            df = read_data_with_dynamic_header(os.path.join(INPUT_DIR, fname), flag)
        except ValueError as e:
            print(f"[{slug}] {e} → skipping")
        if df is None or flag not in df.columns:
            print(f"[{slug}] flag column not found → skipping")
            continue

        # Find only rows where the researcher is flagged as the first appearance
        flagged = df.loc[
            (df[flag] == True) |
            (df[flag].astype(str).str.upper() == 'TRUE')
        ]
        if flagged.empty:
            print(f"[{slug}] no flagged rows → skipping")
            continue

        # Prepare containers for summary and detail
        matched_profiles = []  # to store distinct display names
        all_publications = []  # to collect every publication record

        # For each flagged author entry, validate and fetch works
        for _, row in flagged.iterrows():
            # Retrieve the OpenAlex author ID from known column names
            author_col = next((c for c in ['id','openalex_author_id','author_id'] if c in df.columns), None)
            author_id = row.get(author_col) if author_col else None
            if not author_id or pd.isna(author_id):
                print(f"[{slug}] missing author ID → skipping")
                continue

            # Fetch profile to get display_name and works_api_url
            profile = fetch_json(BASE_AUTHOR_URL + str(author_id))
            display = profile.get('display_name', '')
            works_url = profile.get('works_api_url')

            # Confirm at least one surname matches the profile name
            norm = strip_accents(display).lower()
            if not any(s in norm for s in surnames):
                print(f"[{slug}] no surname match in '{display}' → skipping")
                continue
            if display not in matched_profiles:
                matched_profiles.append(display)
            print(f"[{slug}] matched profile '{display}'")

            # Retrieve and accumulate all publications for this author
            pubs = get_all_works(works_url)
            all_publications.extend(pubs)

        if not matched_profiles:
            print(f"[{slug}] no valid profiles → skipping")
            continue

        # Build DataFrame of detailed works
        details = pd.DataFrame([
            {'title': w.get('title'),
             'doi': w.get('doi'),
             'publication_year': w.get('publication_year'),
             'cited_by_count': w.get('cited_by_count')}
            for w in all_publications
        ])

        # Calculate summary totals from retrieved works
        count_pubs = len(details)
        count_cites = details['cited_by_count'].fillna(0).sum()

        # Create metadata table with aggregated display names and totals
        summary = pd.DataFrame([
            {'display_name': ', '.join(matched_profiles),
             'works_count': count_pubs,
             'cited_by_count': count_cites}
        ])

        # Save both summary and detailed tables as separate sheets
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            summary.to_excel(writer, sheet_name='metadata', index=False)
            details.to_excel(writer, sheet_name='works', index=False)

        print(f"[{slug}] wrote {count_pubs} works (citations: {count_cites})")

    print("All done.")


if __name__ == '__main__':
    # Handle '--clear-dir' flag to reset the output folder
    parser = argparse.ArgumentParser()
    parser.add_argument('--clear-dir', action='store_true', help='wipe output folder')
    args = parser.parse_args()
    if args.clear_dir and os.path.isdir(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Kick off the main processing
    process_files()
