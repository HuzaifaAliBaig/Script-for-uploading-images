import csv
import os
import io
import time
import json
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("script.log"),
        logging.StreamHandler()
    ]
)

# Constants for API scopes
GOOGLE_DRIVE_SCOPE = ['https://www.googleapis.com/auth/drive']

# Zoho CRM credentials from environment variables
ZOHO_REFRESH_TOKEN = "1000.0c28c5dcc37b8a49d800b5a7ca37fcd6.6702d560fccfba716546ba4527a1bae4"
ZOHO_CLIENT_ID = "1000.752PQ5GZY3S2SKSKF60CE6LWY0DHTK"
ZOHO_CLIENT_SECRET = "89fb8bce9fe707b3eb40b325d392667e05edf4b6c7"
ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

ZOHO_CRM_DOMAIN = 'https://www.zohoapis.com'

# Batch processing constants
BATCH_SIZE = 50  # Number of rows per batch
PROGRESS_FILE = 'progress.txt'  # File to save progress
UPLOADED_FILES_FILE = 'uploaded_files.json'  # File to save uploaded File IDs

# Global variable to store the access token
access_token = None

# In-memory cache to store folder_name -> folder_id mappings
folder_cache = {}

def authenticate_google_drive():
    """Authenticate and build the Google Drive service."""
    service_account_file = '/etc/secrets/Google_Key.json'
    if not service_account_file or not os.path.exists(service_account_file):
        logging.error("Service account file path not set or file does not exist.")
        exit(1)
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=GOOGLE_DRIVE_SCOPE)
    drive_service = build('drive', 'v3', credentials=credentials)
    logging.info("Authenticated with Google Drive API.")
    return drive_service

def read_csv_file(csv_file_path):
    """Read CSV file and return list of rows as dictionaries."""
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)  # Comma is the default delimiter
        rows = [row for row in reader]
    logging.info(f"Read {len(rows)} rows from CSV file.")
    return rows

def folder_exists(drive_service, folder_name, parent_folder_id=None):
    """
    Check if a folder exists in Google Drive.
    Returns the folder ID if exists, else None.
    """
    # Check cache first
    if folder_name in folder_cache:
        logging.debug(f"Cache hit for folder '{folder_name}'.")
        return folder_cache[folder_name]
    
    # Escape single quotes in folder_name
    folder_name_escaped = folder_name.replace("'", "\\'")
    
    if parent_folder_id:
        query = f"name = '{folder_name_escaped}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_folder_id}' in parents"
    else:
        query = f"name = '{folder_name_escaped}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    
    logging.debug(f"Checking if folder exists with query: {query}")
    
    try:
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=1000  # Increase page size to ensure all folders are checked
        ).execute()
        
        files = response.get('files', [])
        logging.info(f"Found {len(files)} folders matching '{folder_name}'.")
        
        if files:
            folder_id = files[0]['id']
            folder_cache[folder_name] = folder_id  # Update cache
            return folder_id
        else:
            return None
    except Exception as e:
        logging.error(f"Error checking folder existence: {e}")
        return None

def create_folder(drive_service, folder_name, parent_folder_id=None):
    """Create a folder in Google Drive and return its ID."""
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_folder_id:
        file_metadata['parents'] = [parent_folder_id]
    try:
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        folder_id = folder.get('id')
        folder_cache[folder_name] = folder_id  # Update cache
        logging.info(f"Created folder '{folder_name}' with ID: {folder_id}")
        
        # **Added 2-Second Delay After Folder Creation**
        time.sleep(2)  # Delay to ensure the folder is fully registered in Google Drive
        
        return folder_id
    except Exception as e:
        logging.error(f"Error creating folder '{folder_name}': {e}")
        return None

def fetch_file_from_zoho(file_id):
    """Fetch file data from Zoho CRM."""
    global access_token
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    url = f'{ZOHO_CRM_DOMAIN}/crm/v3/files'
    params = {'id': file_id}
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 401:
        logging.warning("Access token expired. Refreshing token...")
        access_token = refresh_access_token()
        if access_token:
            headers['Authorization'] = f'Zoho-oauthtoken {access_token}'
            response = requests.get(url, headers=headers, params=params)
        else:
            raise Exception("Failed to refresh access token.")
    
    response.raise_for_status()
    logging.debug(f"Fetched file from Zoho CRM with ID: {file_id}")
    return response.content

def upload_file_to_drive(drive_service, folder_id, file_name, file_data):
    """Upload a file to Google Drive under the specified folder."""
    file_metadata = {
        'name': file_name,
        'parents': [folder_id],
    }
    media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='image/jpeg')
    try:
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        uploaded_file_id = file.get('id')
        logging.info(f"Uploaded file '{file_name}' to folder ID: {folder_id} with ID: {uploaded_file_id}")
        return uploaded_file_id
    except Exception as e:
        logging.error(f"Error uploading file '{file_name}': {e}")
        return None

def read_progress():
    """Read the progress index from the progress file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try:
                index = int(f.read().strip())
                logging.info(f"Resuming from index {index}.")
                return index
            except ValueError:
                logging.error("Progress file is corrupted. Starting from index 0.")
                return 0
    else:
        logging.info("No progress file found. Starting from index 0.")
        return 0

def save_progress(index):
    """Save the current progress index to the progress file."""
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(index))
    logging.info(f"Progress saved at index {index}.")

def refresh_access_token():
    """Refresh the Zoho CRM access token using the refresh token."""
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        response = requests.post(ZOHO_TOKEN_URL, data=payload, headers=headers)
        if response.status_code == 200:
            token_data = response.json()
            new_access_token = token_data.get("access_token")
            expires_in = int(token_data.get("expires_in"))
            logging.info(f"New Access Token obtained. Expires in {expires_in / 60} minutes.")
            return new_access_token
        else:
            logging.error(f"Failed to refresh token: {response.json()}")
            return None
    except Exception as e:
        logging.error(f"Exception during token refresh: {e}")
        return None

def load_uploaded_file_ids():
    """Load the set of uploaded File IDs from the JSON file."""
    if os.path.exists(UPLOADED_FILES_FILE):
        try:
            with open(UPLOADED_FILES_FILE, 'r') as f:
                uploaded_ids = set(json.load(f))
            logging.info(f"Loaded {len(uploaded_ids)} uploaded File IDs from '{UPLOADED_FILES_FILE}'.")
            return uploaded_ids
        except Exception as e:
            logging.error(f"Error loading uploaded File IDs: {e}")
            return set()
    else:
        logging.info(f"No '{UPLOADED_FILES_FILE}' found. Starting with an empty set.")
        return set()

def save_uploaded_file_ids(uploaded_ids):
    """Save the set of uploaded File IDs to the JSON file."""
    try:
        with open(UPLOADED_FILES_FILE, 'w') as f:
            json.dump(list(uploaded_ids), f, indent=4)
        logging.info(f"Saved {len(uploaded_ids)} uploaded File IDs to '{UPLOADED_FILES_FILE}'.")
    except Exception as e:
        logging.error(f"Error saving uploaded File IDs: {e}")

def main():
    global access_token
    csv_file_path = 'updated.csv'
    parent_folder_id = '1VeR-E_NVIGFvYi68U_0kOKkShTVzQUUr'  # Replace with your parent folder ID
    
    # Authenticate Google Drive API
    drive_service = authenticate_google_drive()

    # Read CSV data
    rows = read_csv_file(csv_file_path)
    total_rows = len(rows)

    # Read progress
    start_index = read_progress()
    end_index = total_rows  # Process till the end

    logging.info(f"Processing rows from index {start_index} to {end_index - 1}")

    # Initialize folder cache by listing existing folders under the parent
    try:
        existing_folders = drive_service.files().list(
            q=f"mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_folder_id}' in parents",
            spaces='drive',
            fields='files(id, name)',
            pageSize=1000
        ).execute()
        for folder in existing_folders.get('files', []):
            folder_cache[folder['name']] = folder['id']
        logging.info(f"Cached {len(folder_cache)} existing folders.")
    except Exception as e:
        logging.error(f"Error caching existing folders: {e}")

    # Load uploaded File IDs
    uploaded_file_ids = load_uploaded_file_ids()

    # Refresh access token before starting
    access_token = refresh_access_token()
    if not access_token:
        logging.error("Failed to obtain access token. Exiting...")
        exit(1)

    rows_processed = 0

    for i in range(start_index, end_index):
        row = rows[i]
        file_id_s = row.get('File_Id__s', '').strip()
        if not file_id_s:
            logging.warning(f"Row {i}: 'File_Id__s' is empty. Skipping.")
            continue  # Skip rows where 'File_Id__s' is empty

        # **Check if File ID has already been uploaded**
        if file_id_s in uploaded_file_ids:
            logging.info(f"Row {i}: File ID '{file_id_s}' has already been uploaded. Skipping.")
            continue  # Skip already uploaded files

        # Construct folder name using Full_Name, Mailing_Street, and Well_Id without stripping
        full_name = row.get('Full_Name', '').strip()
        mailing_street = row.get('Mailing_Street', '').strip()
        well_id = row.get('Well_Id', '').strip()
        location = row.get('Image Field', '').strip()

        folder_name = f"{full_name}_{mailing_street}_{well_id}"

        # Check if folder exists
        folder_id = folder_exists(drive_service, folder_name, parent_folder_id)
        if not folder_id:
            # Create folder if it doesn't exist
            folder_id = create_folder(drive_service, folder_name, parent_folder_id)
            if not folder_id:
                logging.error(f"Failed to create or retrieve folder '{folder_name}'. Skipping row {i}.")
                continue  # Skip to the next row if folder creation fails
        else:
            logging.debug(f"Folder '{folder_name}' exists with ID: {folder_id}")

        # Fetch file from Zoho CRM
        try:
            file_data = fetch_file_from_zoho(file_id_s)
            logging.info(f"Fetched file from Zoho CRM with ID: {file_id_s}")
        except requests.HTTPError as e:
            logging.error(f"Failed to fetch file from Zoho CRM with ID: {file_id_s}. Error: {e}")
            continue  # Skip to the next row
        except Exception as e:
            logging.error(f"Unexpected error fetching file ID {file_id_s}: {e}")
            continue  # Skip to the next row

        # Construct file name
        file_name = f"{location}_{file_id_s}.jpg"

        # Upload file to Google Drive
        try:
            uploaded_file_id = upload_file_to_drive(drive_service, folder_id, file_name, file_data)
            if not uploaded_file_id:
                logging.error(f"Failed to upload file '{file_name}' to Google Drive.")
                continue  # Skip to the next row
        except Exception as e:
            logging.error(f"Failed to upload file '{file_name}' to Google Drive. Error: {e}")
            continue  # Optionally continue to next row

        # **Add the File ID to the uploaded list and save**
        uploaded_file_ids.add(file_id_s)
        save_uploaded_file_ids(uploaded_file_ids)

        # **Add a 1-Second Delay After Each Upload**
        time.sleep(1)

        rows_processed += 1
        current_index = i + 1  # Next index to process

        # Refresh access token after every 50 rows
        if rows_processed % BATCH_SIZE == 0:
            logging.info(f"Processed {rows_processed} rows. Refreshing access token...")
            new_token = refresh_access_token()
            if new_token:
                access_token = new_token
            else:
                logging.error("Failed to refresh access token. Exiting...")
                break  # Exit the loop if token refresh fails

            # Optional: Save progress after each batch
            save_progress(current_index)

    # Save progress after processing all rows
    if rows_processed > 0:
        save_progress(start_index + rows_processed)

    # If we've reached the end of the data, delete the progress file
    if start_index + rows_processed >= total_rows:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            logging.info("Processing complete. Progress file removed.")
        else:
            logging.info("Processing complete.")
    else:
        logging.info(f"Processing stopped at index {start_index + rows_processed}.")

if __name__ == '__main__':
    main()
