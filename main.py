import csv
import os
import io
import time
import json
import requests
from threading import Thread
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account

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
NUM_BATCHES_PER_RUN = 2  # Number of batches to process per script run
PROGRESS_FILE = 'progress.txt'  # File to save progress

# Global variable to store the access token
access_token = None

def authenticate_google_drive():
    # Read the service account JSON file from an environment variable
    service_account_file = 'safewell-442418-81f1d6c69af0.json'
    if not service_account_file or not os.path.exists(service_account_file):
        print("Error: Service account file path not set or file does not exist.")
        exit(1)
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=GOOGLE_DRIVE_SCOPE)
    drive_service = build('drive', 'v3', credentials=credentials)
    return drive_service

def read_csv_file(csv_file_path):
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)  # Comma is the default delimiter
        rows = [row for row in reader]
    return rows

def folder_exists(drive_service, folder_name, parent_folder_id=None):
    # Adjusted search query to include parent folder ID if provided
    if parent_folder_id:
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_folder_id}' in parents"
    else:
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']  # Return the first matching folder ID
    else:
        return None

def create_folder(drive_service, folder_name, parent_folder_id=None):
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_folder_id:
        file_metadata['parents'] = [parent_folder_id]
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def fetch_file_from_zoho(file_id):
    global access_token
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    url = f'{ZOHO_CRM_DOMAIN}/crm/v3/files'
    params = {'id': file_id}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 401:
        print("Access token expired. Refreshing token...")
        access_token = refresh_access_token()
        if access_token:
            headers['Authorization'] = f'Zoho-oauthtoken {access_token}'
            response = requests.get(url, headers=headers, params=params)
        else:
            raise Exception("Failed to refresh access token.")
    response.raise_for_status()
    return response.content

def upload_file_to_drive(drive_service, folder_id, file_name, file_data):
    file_metadata = {
        'name': file_name,
        'parents': [folder_id],
    }
    media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='image/jpeg')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

def read_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            index = int(f.read().strip())
    else:
        index = 0
    return index

def save_progress(index):
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(index))

def refresh_access_token():
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(ZOHO_TOKEN_URL, data=payload, headers=headers)
    
    if response.status_code == 200:
        token_data = response.json()
        new_access_token = token_data.get("access_token")
        expires_in = int(token_data.get("expires_in"))
        print(f"New Access Token obtained. Expires in {expires_in / 60} minutes.")
        return new_access_token
    else:
        print("Failed to refresh token:", response.json())
        return None

def token_refresher():
    global access_token
    while True:
        access_token = refresh_access_token()
        if access_token:
            # Wait for 40 minutes before refreshing again
            time.sleep(40 * 60)
        else:
            print("Failed to obtain access token. Retrying in 1 minute...")
            time.sleep(60)

def main():
    csv_file_path = 'updated_file2.csv'
    parent_folder_id = '1VeR-E_NVIGFvYi68U_0kOKkShTVzQUUr'# Replace with your parent folder ID or set as an environment variable
    
    # Authenticate Google Drive API
    drive_service = authenticate_google_drive()

    # Read CSV data
    rows = read_csv_file(csv_file_path)
    total_rows = len(rows)

    # Read progress
    start_index = read_progress()
    end_index = start_index + BATCH_SIZE * NUM_BATCHES_PER_RUN

    # Ensure end_index does not exceed total rows
    if end_index > total_rows:
        end_index = total_rows

    print(f"Processing rows from index {start_index} to {end_index - 1}")

    for i in range(start_index, end_index):
        row = rows[i]
        file_id_s = row.get('File_Id__s', '')
        if not file_id_s:
            continue  # Skip rows where 'File_Id__s' is empty

        # Use 'File_Id__s' as-is
        file_id_s_extracted = file_id_s

        # Construct folder name using Full_Name, Mailing_Street, and Well_Id without stripping
        full_name = row.get('Full_Name', '')
        mailing_street = row.get('Mailing_Street', '')
        well_id = row.get('Well_Id', '')
        location = row.get('Image Field','')

        # Include all components as they are, including whitespace and special characters
        folder_name = f"{full_name}_{mailing_street}_{well_id}"

        # Check if folder exists
        folder_id = folder_exists(drive_service, folder_name, parent_folder_id)
        if not folder_id:
            # Create folder if it doesn't exist
            folder_id = create_folder(drive_service, folder_name, parent_folder_id)
            print(f"Created folder '{folder_name}' with ID: {folder_id}")
        else:
            print(f"Folder '{folder_name}' already exists with ID: {folder_id}")

        # Fetch file from Zoho CRM
        try:
            file_data = fetch_file_from_zoho(file_id_s_extracted)
            print(f"Fetched file from Zoho CRM with ID: {file_id_s_extracted}")
        except requests.HTTPError as e:
            print(f"Failed to fetch file from Zoho CRM with ID: {file_id_s_extracted}. Error: {e}")
            continue  # Skip to the next row

        # Construct file name
        file_name = f"{location}_{file_id_s_extracted}.jpg"

        # Upload file to Google Drive
        try:
            uploaded_file_id = upload_file_to_drive(drive_service, folder_id, file_name, file_data)
            print(f"Uploaded file '{file_name}' to folder '{folder_name}' with ID: {uploaded_file_id}")
        except Exception as e:
            print(f"Failed to upload file '{file_name}' to Google Drive. Error: {e}")
            continue  # Optionally continue to next row

    # Save progress
    save_progress(end_index)

    # If we've reached the end of the data, delete the progress file
    if end_index >= total_rows:
        os.remove(PROGRESS_FILE)
        print("Processing complete. Progress file removed.")

if __name__ == '__main__':
    # Start the token refresher in a separate thread
    token_thread = Thread(target=token_refresher)
    token_thread.daemon = True  # This will allow the program to exit even if the thread is running
    token_thread.start()

    # Wait a moment to ensure the access token is obtained before starting main processing
    time.sleep(5)
    if not access_token:
        print("Failed to obtain access token. Exiting...")
        exit(1)

    main()
