import requests
import sqlite3
import os
import csv
from datetime import datetime

# --- SUBMISSION CONFIGURATION ---
STUDENT_ID = "23262136" 
DB_NAME = f"{STUDENT_ID}-seeding.db" 

REPOSITORIES = {
    "Dataverse.no": "https://dataverse.no/api",
    "Harvard Murray": "https://dataverse.harvard.edu/api"
}

SAVE_DIR = "./qdarchive_storage"
CSV_NAME = "metadata_export.csv"
QDA_EXTENSIONS = ['.qdpx', '.qdpz', '.qdx', '.mx24']

def init_env():
    """Initializes the database with the EXACT schema required by the validator."""
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Fixed PROJECTS Table (Matches validator requirements)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            description TEXT,
            project_url TEXT,
            repository_id TEXT,
            repository_url TEXT,
            download_date TEXT,
            download_method TEXT,
            download_project_folder TEXT,
            download_repository_folder TEXT
        )
    ''')

    # 2. Required Supplementary Tables (Fixes schema.tables.required)
    cursor.execute('CREATE TABLE IF NOT EXISTS FILES (id INTEGER PRIMARY KEY, project_id INTEGER, status TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS KEYWORDS (id INTEGER PRIMARY KEY, project_id INTEGER, keyword TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS LICENSES (id INTEGER PRIMARY KEY, project_id INTEGER, license TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS PERSON_ROLE (id INTEGER PRIMARY KEY, project_id INTEGER, name TEXT, role TEXT)')
    
    conn.commit()
    return conn

def acquire_project(base_url, repo_name, doi):
    """Downloads project and logs data using the correct schema."""
    conn = init_env()
    cursor = conn.cursor()
    print(f"\n🚀 Processing: {doi}")
    
    try:
        # 1. Fetch Metadata
        res = requests.get(f"{base_url}/datasets/:persistentId/?persistentId={doi}").json()
        if 'data' not in res: return
        version = res['data']['latestVersion']
        metadata_fields = version['metadataBlocks']['citation']['fields']

        # 2. Extract Required Fields
        title = ""
        description = ""
        for field in metadata_fields:
            if field['typeName'] == 'title':
                title = field['value']
            if field['typeName'] == 'dsDescription':
                description = field['value'][0]['dsDescriptionValue']['value']

        # 3. License Check
        license_info = version.get('license', 'No License Found')
        if isinstance(license_info, dict): license_info = license_info.get('name', 'No License Found')
        if "restricted" in str(license_info).lower(): return

        # 4. Download Logic
        folder_name = doi.replace("/", "_").replace(":", "_")
        local_path = os.path.join(SAVE_DIR, folder_name)
        
        dl_url = f"{base_url}/access/dataset/:persistentId/?persistentId={doi}"
        file_res = requests.get(dl_url, stream=True)
        
        if file_res.status_code == 200 and 'zip' in file_res.headers.get('Content-Type', ''):
            os.makedirs(local_path, exist_ok=True)
            zip_path = os.path.join(local_path, "project_bundle.zip")
            with open(zip_path, 'wb') as f:
                for chunk in file_res.iter_content(chunk_size=8192):
                    f.write(chunk)

            # 5. Insert into PROJECTS (Matches Validator Schema)
            cursor.execute('''
                INSERT INTO projects (
                    title, description, project_url, repository_id, repository_url, 
                    download_date, download_method, download_project_folder, download_repository_folder
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                title, 
                description, 
                f"https://doi.org/{doi.replace('doi:', '')}", 
                doi, 
                base_url, 
                datetime.now().isoformat(), 
                "API_Heuristic", 
                local_path, 
                SAVE_DIR
            ))
            
            # 6. Fill LICENSES table (to satisfy validator warnings)
            project_id = cursor.lastrowid
            cursor.execute('INSERT INTO LICENSES (project_id, license) VALUES (?, ?)', (project_id, str(license_info)))
            
            conn.commit()
            print(f"✅ Saved to DB with ID: {project_id}")

    except Exception as e:
        print(f"⚠️ Technical challenge: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    search_terms = ["qdpx", "NVivo", "MAXQDA"]
    for name, api_url in REPOSITORIES.items():
        for term in search_terms:
            res = requests.get(f"{api_url}/search?q={term}&type=dataset").json()
            dois = [item.get('global_id') for item in res.get('data', {}).get('items', [])]
            for doi in dois:
                acquire_project(api_url, name, doi)