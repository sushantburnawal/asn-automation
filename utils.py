from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime
import os
import re
import pandas as pd
from google.cloud import storage
import logging


GCS_BUCKET = "asn-automation"
GCS_BASE_PATH = "gcs_files/invoices/"

def upload_file_to_gcs(file_path: str, email_address: str = None) -> dict:
    """
    Upload the file to Google Cloud Storage and return a reference dictionary
    that can be used by Vertex AI.
    """
    # Define your GCS bucket and base path (update these as needed)
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)

        # Construct a more descriptive GCS path using the email address, if available
        if email_address:
            destination_blob_name = os.path.join(GCS_BASE_PATH, email_address, os.path.basename(file_path))
        else:
            destination_blob_name = os.path.join(GCS_BASE_PATH, os.path.basename(file_path))

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        # Return a file reference as expected by Vertex AI.
        return {"gcsUri": f"gs://{GCS_BUCKET}/{destination_blob_name}"}
    except Exception as e:
        logging.error(f"Error uploading file {file_path} to GCS: {e}")
        raise e
    

def send_email_with_sender_fallback(recipient_emails, subject, body, attachment_path,
                                    sender_credentials_list,
                                    smtp_server="smtp.gmail.com",
                                    smtp_port=587):
    """
    Tries sending the email using each sender in sender_credentials_list until one succeeds.
    
    :param recipient_email: Email recipient (or list of recipients)
    :param subject: Email subject line
    :param body: Email body text
    :param attachment_path: Path to attachment file
    :param sender_credentials_list: List of tuples (email_sender, email_password)
    :param smtp_server: SMTP server to use (default is Gmail's)
    :param smtp_port: SMTP port to use (default 587)
    """
    for email_sender, email_password in sender_credentials_list:
        try:
            send_email(recipient_emails, subject, body, attachment_path,
                       smtp_server=smtp_server,
                       smtp_port=smtp_port,
                       email_sender=email_sender,
                       email_password=email_password)
            print(f"[LOG] Email sent successfully using sender {email_sender} for subject '{subject}'.")
            return  # Stop if sending is successful
        except Exception as e:
            print(f"[LOG] ERROR sending email using sender {email_sender}: {e}. Trying next fallback sender email.")
    
    print(f"[LOG] All sender email addresses failed for subject '{subject}'.")

def send_email(to_email, subject, body, attachment_path,
               smtp_server="smtp.gmail.com",
               smtp_port=587,
               email_sender="asnautomationtool@zeptonow.com",
               email_password="gmig chyn osfh qenm"):
    """
    Sends an email via SMTP with a single attachment.
    Supports passing to_email as a list of recipient addresses.
    """
    from email.message import EmailMessage
    import smtplib
    import os

    msg = EmailMessage()
    msg["From"] = email_sender

    # If to_email is a list, join them into a comma-separated string.
    if isinstance(to_email, list):
        msg["To"] = ", ".join(to_email)
    else:
        msg["To"] = to_email

    msg["Subject"] = subject
    msg.set_content(body)

    if attachment_path and os.path.isfile(attachment_path):
        with open(attachment_path, "rb") as f:
            msg.add_attachment(f.read(),
                               maintype="application",
                               subtype="csv",
                               filename=os.path.basename(attachment_path))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(email_sender, email_password)
        server.send_message(msg)

def get_existing_po_codes():
    """
    Reads the "Output" worksheet and returns a set of PO codes that have already been processed.
    Assumes that the PO code is stored under the header "PO No".
    """
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(f"chhavi_443110_7d0706dd8efc.json", scope)

    gc = gspread.authorize(creds)
    link = 'https://docs.google.com/spreadsheets/d/1V3iL0KQ9ADk67R5pJO1rSVowpFWw-v_YQfyplaY9M_8/edit?gid=0#gid=0'
    ws_output = gc.open_by_url(link).worksheet("Output")
    # Get all records as a list of dicts
    records = ws_output.get_all_records()
    existing = set()
    for rec in records:
        # Adjust the key ("PO No") if your sheet uses a different header name
        po_val = rec.get("PO number")
        if po_val:
            existing.add(str(po_val))
    return existing

