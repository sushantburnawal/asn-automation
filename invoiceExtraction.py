import os
import email
from imapclient import IMAPClient
from datetime import datetime, timedelta

# These packages are used to read the Google Sheet.
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import fitz
import shutil
import time
import pandas as pd
from utils import upload_file_to_gcs

############################################
# 1) Get Manufacturer List from Google Sheets
############################################

def get_manufs_final():
    """
    Reads the 'Dash' worksheet from a Google Sheet and returns a list of rows (as dictionaries)
    for which 'run_flag' == 'Yes'. Make sure your sheet has a column named 'manufacturer' (and optionally
    others if needed).
    """
    # Define the scope and path to your service account JSON file
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Update with the folder where your JSON credentials are stored and the JSON filename
    creds = ServiceAccountCredentials.from_json_keyfile_name(f"chhavi_443110_7d0706dd8efc.json", scope)
    
    # Authorize and open the Google Sheet
    gc = gspread.authorize(creds)
    # Update with your actual Google Sheet URL and worksheet name
    sheet_url = "https://docs.google.com/spreadsheets/d/1V3iL0KQ9ADk67R5pJO1rSVowpFWw-v_YQfyplaY9M_8/edit?gid=0#gid=0"
    worksheet_name = "Bevarage"
    ws = gc.open_by_url(sheet_url).worksheet(worksheet_name)
    
    # Get the data as a DataFrame and remove completely empty rows
    df = gd.get_as_dataframe(ws, evaluate_formulas=True, usecols=lambda x: x not in [''], header=0)
    
    df.dropna(how='all', inplace=True)
   
    
    # Convert the DataFrame to a list of dictionaries (each representing a manufacturer row)
    return df.to_dict(orient='records')

def copy_files(src_dir, dest_dir, file_list):
    """Copy specified files from src_dir to dest_dir with metadata."""
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    for file in file_list:
        src_path = os.path.join(src_dir, file)
        dest_path = os.path.join(dest_dir, file)
        if os.path.exists(src_path):
            if os.path.exists(dest_path) or os.path.lexists(dest_path):
                try:
                    os.remove(dest_path)
                    time.sleep(1)
                except Exception as e:
                    print(f"[LOG] Error removing {dest_path}: {e}")
                
            try:
                shutil.copy2(src_path, dest_path)
            except FileExistsError as fe:
                print(f"[LOG] FileExistsError even after removal for {dest_path}: {fe}")

        else:
            print(f"[LOG] File not found: {src_path}")

def pdf_contains_pattern(pdf_path, pattern):
        doc = fitz.open(pdf_path)
        found = False
        for page_idx, page in enumerate(doc):
            text = page.get_text("text")
            if pattern in text:
                found = True
                break
        doc.close()
        return found

def process_email_row(row, since_hours=1, base_local_folder="downloaded_attachments"):
    """
    For a given manufacturer row, this function logs into the mailbox (using common credentials),
    searches for emails since a specified number of hours ago, and saves attachments (PDF, CSV, XLSX, XLS)
    into a manufacturer-specific subfolder under the base_local_folder.
    
    :param row: Dictionary containing at least the key "manufacturer". (Other fields can be present if needed.)
    :param since_hours: How many hours back to search for emails.
    :param base_local_folder: Base folder where attachments will be saved.
    """
    # manufacturer     = row["manufacturer"]
    email_address    = row["email"]
    password         = row["password"]
    # pattern_str      = row["pattern"]
    # po_number_col    = row["po_number"]
    # invoice_num_col  = row["invoice_number"]
    # zepto_pvid_col   = row["zepto_pvid"]
    # invoice_qty_col  = row["invoice_quantity"]
    # zepto_pvid_type  = row["zepto_pvid_Type"]
    # delete_folders   = row.get("delete_folders", "No")
    
    TRUE_DIR = f"{base_local_folder}/{email_address}_TRUE"
    RAW_DIR  = f"{base_local_folder}/{email_address}_RAW"
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(TRUE_DIR, exist_ok=True)
    
    #print(f"[INFO] Processing manufacturer '{manufacturer}' using email '{email_address}'")
    
    # Connect to the IMAP server (Gmail is used here as an example)
    IMAP_SERVER = "imap.gmail.com"
    with IMAPClient(IMAP_SERVER) as client:
        try:
            client.login(email_address, password)
            client.select_folder("INBOX", readonly=True)
        
            # Calculate the date string for the time window (e.g., emails received since the last hour)
            since_date = (datetime.now() - timedelta(hours=since_hours)).strftime("%d-%b-%Y")
            messages = client.search(["SINCE", since_date,'NOT','FROM','@zeptonow.com'])
            print(f"[INFO] Found {len(messages)} email(s) for '{email_address}' since {since_date}.")
            
            # Process each email found
            for msgid, data in client.fetch(messages, ["RFC822"]).items():
                msg = email.message_from_bytes(data[b"RFC822"])
                subject = msg.get("subject", "No_Subject")
                # Create a sanitized version of the subject to use in filenames
                safe_subject = "".join(c if c.isalnum() or c in (" ", "_") else "" for c in subject).strip().replace(" ", "_")
                
                # Walk through the parts of the email to find attachments
                for part in msg.walk():
                    # Skip container parts
                    if part.get_content_maintype() == "multipart":
                        continue
                    
                    content_disp = part.get("Content-Disposition", "")
                    if "attachment" in content_disp.lower():
                        filename = part.get_filename()
                        if not filename:
                            continue
                        
                        filename_lower = filename.lower()
                        # Only process attachments with the desired file types
                        if filename_lower.endswith((".pdf")):
                            final_filename = f"{safe_subject}_{filename}"
                            save_path = os.path.join(RAW_DIR, final_filename)
                            with open(save_path, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            
                            # Upload the PDF to GCS
                            try:
                                upload_file_to_gcs(save_path, email_address)
                                print(f"[INFO] Uploaded {final_filename} to GCS.")
                            except Exception as gcs_error:
                                print(f"[ERROR] Failed to upload {final_filename} to GCS: {gcs_error}")

        except Exception as e:
            print(f"[ERROR] Error logging into {email_address}: {e}")
                       #print(f"[INFO] Saved attachment for '{manufacturer}' to: {save_path}")
                        

def main():
    # Get the list of manufacturers from the Google Sheet
    manufs = get_manufs_final()
    
    if not manufs:
        print("[INFO] No manufacturers found to process.")
        return
    
    print(f"[INFO] Found {len(manufs)} emails to process.")
    
    # Process each manufacturer row. (You can later parallelize this if needed.)
    for row in manufs:
        try:
            process_email_row(row, since_hours=1, base_local_folder="downloaded_attachments")
        except Exception as e:
            print(f"[ERROR] Error processing email: {e}")

if __name__ == "__main__":
    main()