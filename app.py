import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# Google Sheets setup
def get_google_sheet_client(credentials_path, sheet_id):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

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

# Search for alternative campaign names in Ahmedsettings sheet using the new structure
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

    sheet_id = "1kldmjmZmtpvMbz_Jqb4pyjinOIYFke3jpYciWXGl91Y"
    credentials_path = "Credentials.json"
    sheet_name = st.text_input("Enter Google Sheet name (as it appears in Google Sheets):")

    update_type = st.selectbox("Select Update Type:", ["CTC", "Log type"])

    uploaded_file = st.file_uploader("Upload CSV file", type="csv")

    day_index = st.selectbox("Select the day of the week:", list(range(1, 6))) - 1

    if st.button("Execute") and uploaded_file:
        if update_type == "CTC":
            connects_target_df = load_and_process_csv(uploaded_file)
            target_column_label = "CTC"
            update_columns = ["Calls", "CTC", "Abandoned", "Connects"]
        else:
            connects_target_df = process_campaign_data_by_name(uploaded_file)
            target_column_label = "Logged Calls"
            update_columns = ["Logged Calls", "Dial Time"]

        st.write("Aggregated Data Preview:", connects_target_df.head())

        try:
            workbook = get_google_sheet_client(credentials_path, sheet_id)
            worksheet = workbook.worksheet(sheet_name)
            settings_sheet = workbook.worksheet("AhmedSettings")
            settings_data = settings_sheet.get_all_records()
            settings_df = pd.DataFrame(settings_data)

            data = worksheet.get_all_values()
            header_row = data[1]

            # Map each column to its specified day index
            target_columns = {col: get_column_index(col, day_index, header_row) for col in update_columns}

            if None in target_columns.values():
                st.error("Invalid day index for one or more columns. Please check the sheet headers.")
            else:
                for _, row in connects_target_df.iterrows():
                    camp_name = row['Camp']
                    logged_calls = row.get("Logged Calls", None)
                    dial_time = row.get("Dial Time", None)

                    # Locate the row with the campaign name
                    camp_row_index = None
                    for j, row_data in enumerate(data):
                        if row_data[0].strip() == camp_name:
                            camp_row_index = j + 1
                            break

                    # If campaign not found, check Ahmedsettings for alternative names
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

                    # Update Google Sheets if a row is found
                    if camp_row_index:
                        if update_type == "CTC":
                            worksheet.update_cell(camp_row_index, target_columns["Calls"], row['Calls'])
                            worksheet.update_cell(camp_row_index, target_columns["Connects"], row['Connects'])
                            worksheet.update_cell(camp_row_index, target_columns["CTC"], row['CTC'])
                            worksheet.update_cell(camp_row_index, target_columns["Abandoned"], row['Abandoned'])
                        else:
                            worksheet.update_cell(camp_row_index, target_columns["Logged Calls"], logged_calls)
                            if "Dial Time" in target_columns:
                                worksheet.update_cell(camp_row_index, target_columns["Dial Time"], dial_time)
                        st.success(f"Updated {camp_name} on day {day_index + 1} with targets.")
                    else:
                        st.warning(f"Camp name '{camp_name}' and its alternatives not found in the sheet.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    elif not uploaded_file:
        st.warning("Please upload a CSV file before executing.")

if __name__ == "__main__":
    main()
