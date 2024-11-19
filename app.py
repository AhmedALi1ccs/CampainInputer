import json
import os
import time
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Google Sheets setup
def get_google_sheet_client(sheet_id):
    credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not credentials_json:
        raise ValueError("Credentials not found in environment variables")

    credentials_info = json.loads(credentials_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

# Retry logic with exponential backoff
def safe_update_cell(worksheet, row, col, value, max_retries=7):
    retry_count = 0
    while retry_count < max_retries:
        try:
            worksheet.update_cell(row, col, value)
            return  # Success, exit function
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:  # Too Many Requests
                retry_count += 1
                wait_time = 2 ** retry_count  # Exponential backoff
                st.warning(f"Rate limit hit. Retrying in {wait_time} seconds... ({retry_count}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise e  # Reraise other exceptions
    st.error("Max retries reached. Update failed.")

# Function to find the specific occurrence of a column label based on the day index
def get_column_index(label, index, header_row):
    occurrences = [i for i, cell in enumerate(header_row) if cell.strip() == label]
    return occurrences[index] + 1 if index < len(occurrences) else None  # 1-based indexing

# Load, process, and rename columns in CSV data for CTC type
def load_and_process_csv(file):
    data = pd.read_csv(file)
    data = data.dropna(subset=['Campaign'])
    data = data.rename(columns={
        'Campaign': 'Camp',
        'Calls to Connect': 'CTC'
    })
    aggregated_data = data.groupby('Camp').agg({
        'Calls': 'sum',
        'Connects': 'sum',
        'CTC': 'mean',
        'Abandoned': 'sum'
    }).reset_index()
    return aggregated_data

# Process CSV for "Log type"
def process_campaign_data_by_name(file):
    df = pd.read_csv(file)
    df = df.dropna(subset=['Current campaign', 'Recording Length (Seconds)'])
    df['Recording Length (Seconds)'] = df['Recording Length (Seconds)'].astype(int)
    campaign_summary = df.groupby('Current campaign').agg(
        Recording_Length_Seconds=('Recording Length (Seconds)', 'sum'),
        Logged_Calls=('Current campaign', 'count')
    ).reset_index()

    def seconds_to_hms(seconds):
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    campaign_summary['Dial Time'] = campaign_summary['Recording_Length_Seconds'].apply(seconds_to_hms)
    campaign_summary = campaign_summary.rename(columns={
        'Current campaign': 'Camp',
        'Logged_Calls': 'Logged Calls'
    })
    return campaign_summary

# Search for alternative campaign names in AhmedSettings sheet using the new structure
def find_alternate_campaign_name_with_new_structure(settings_df, campaign_name):
    if campaign_name in settings_df['Camp'].values:
        row_index = settings_df[settings_df['Camp'] == campaign_name].index[0]
        alternate_names = settings_df.iloc[row_index, 1:].dropna().unique().tolist()
        return alternate_names
    else:
        for col in settings_df.columns[1:]:
            if campaign_name in settings_df[col].values:
                row_index = settings_df[settings_df[col] == campaign_name].index[0]
                alternate_names = settings_df.iloc[row_index].dropna().unique().tolist()
                alternate_names = [name for name in alternate_names if name != campaign_name]
                return alternate_names
    return []

# Main Streamlit App
def main():
    st.title("Google Sheets Campaign Updater")

    sheet_id = "17X63rlgieIfCbIi33f1NoqrsccnZyB6vDWZd0JgtgT4"
    sheet_name = st.text_input("Enter Google Sheet name (as it appears in Google Sheets):")
    update_type = st.selectbox("Select Update Type:", ["CTC", "Log type"])
    uploaded_files = st.file_uploader("Upload CSV files", type="csv", accept_multiple_files=True)
    day_index = st.selectbox("Select the day of the week:", list(range(1, 6))) - 1

    if st.button("Execute") and uploaded_files:
        workbook = get_google_sheet_client(sheet_id)
        worksheet = workbook.worksheet(sheet_name)
        settings_sheet = workbook.worksheet("AhmedSettings")
        settings_data = settings_sheet.get_all_records()
        settings_df = pd.DataFrame(settings_data)
        data = worksheet.get_all_values()
        header_row = data[1]

        for uploaded_file in uploaded_files:
            st.write(f"Processing file: {uploaded_file.name}")
            try:
                if update_type == "CTC":
                    connects_target_df = load_and_process_csv(uploaded_file)
                    update_columns = ["Calls", "CTC", "Abandoned", "Connects"]
                else:
                    connects_target_df = process_campaign_data_by_name(uploaded_file)
                    update_columns = ["Logged Calls", "Dial Time"]

                target_columns = {col: get_column_index(col, day_index, header_row) for col in update_columns}

                if None in target_columns.values():
                    st.error("Invalid day index for one or more columns. Please check the sheet headers.")
                    continue

                for _, row in connects_target_df.iterrows():
                    camp_name = row['Camp']
                    camp_row_index = None

                    for j, row_data in enumerate(data):
                        if row_data[0].strip() == camp_name:
                            camp_row_index = j + 1
                            break

                    if not camp_row_index:
                        alternate_names = find_alternate_campaign_name_with_new_structure(settings_df, camp_name)
                        for alt_name in alternate_names:
                            for j, row_data in enumerate(data):
                                if row_data[0].strip() == alt_name:
                                    camp_row_index = j + 1
                                    break
                            if camp_row_index:
                                st.info(f"Using alternate name '{alt_name}' for campaign '{camp_name}'.")
                                break

                    if camp_row_index:
                        if update_type == "CTC":
                            safe_update_cell(worksheet, camp_row_index, target_columns["Calls"], row['Calls'])
                            safe_update_cell(worksheet, camp_row_index, target_columns["Connects"], row['Connects'])
                            safe_update_cell(worksheet, camp_row_index, target_columns["CTC"], row['CTC'])
                            safe_update_cell(worksheet, camp_row_index, target_columns["Abandoned"], row['Abandoned'])
                        else:
                            safe_update_cell(worksheet, camp_row_index, target_columns["Logged Calls"], row['Logged Calls'])
                            if "Dial Time" in target_columns:
                                safe_update_cell(worksheet, camp_row_index, target_columns["Dial Time"], row['Dial Time'])
                        st.success(f"Updated {camp_name} from file {uploaded_file.name}")
                    else:
                        st.warning(f"Camp name '{camp_name}' not found in the sheet.")

            except Exception as e:
                st.error(f"An error occurred while processing {uploaded_file.name}: {e}")

if __name__ == "__main__":
    main()
