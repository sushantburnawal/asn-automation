from google import genai
from google.genai.types import Part, HttpOptions
from pydantic import BaseModel, Field, ValidationError
from databricks import sql
from typing import List
import json
from datetime import datetime
import pandas as pd
import json
import os
import pandas as pd
from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from difflib import SequenceMatcher

import logging
import functools
import time
import fitz
import re
from utils import  get_existing_po_codes, composeAndSendEmail, upload_file_to_gcs, send_email, send_email_with_sender_fallback
import multiprocessing
from google.cloud import storage
import io

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler("error.log")
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

GCS_BUCKET = "asn-automation"
GCS_BASE_PATH = "gcs_files/invoices/"


with open("config.json", "r") as f:
    config_data = json.load(f)

# Set up connection details
DATABRICKS_SERVER_HOSTNAME = config_data.get("hostname")
DATABRICKS_ACCESS_TOKEN = config_data.get("accessToken")
DATABRICKS_HTTP_PATH = config_data.get("warehouseHTTPPath")

client = genai.Client(http_options=HttpOptions(api_version="v1"),
  vertexai=True, project="decisive-talon-451317-d6", location="us-central1",
)
model_id =  "gemini-2.0-flash-001" # or "gemini-2.0-flash-lite-preview-02-05"  , "gemini-2.0-pro-exp-02-05"


    
def retry(max_retries=10, backoff_factor=1, exceptions=(Exception,)):
    """
    A decorator that retries a function call with exponential backoff.

    :param max_retries: Maximum number of retries before giving up.
    :param backoff_factor: A factor for computing the delay between retries.
    :param exceptions: A tuple of exceptions that should trigger a retry.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    wait_time = backoff_factor * (2 ** attempt)
                    logging.warning(f"Attempt {attempt+1}/{max_retries} for {func.__name__} failed with error: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
            raise Exception(f"Function {func.__name__} failed after {max_retries} attempts")
        return wrapper
    return decorator

# Example: Wrap the API call for content generation.
@retry(max_retries=10, backoff_factor=1, exceptions=(Exception,))
def generate_content_with_retry(*args, **kwargs):
    return client.models.generate_content(*args, **kwargs)


# Establish connection
conn =sql.connect(
    server_hostname=DATABRICKS_SERVER_HOSTNAME,
    access_token=DATABRICKS_ACCESS_TOKEN,
    http_path=DATABRICKS_HTTP_PATH
)

# Define a Pydantic model for the row structure
class ProductVariant(BaseModel):
    product_variant_id: str
    product_name: str
    description: str

class PVID(BaseModel):
    items: list[ProductVariant] = Field(description="The list of items with description and product_variant_id and product_name ")


class PO(BaseModel):
    product_variant_id: str
    product_name: str


class Item(BaseModel):
    description: str = Field(description="The description of the item")
    quantity: float = Field(description="The Quanitity of the item")
    mrp: float = Field(description="The MRP of the item, STRICTYLY EXTRACT MRP AND NOT NLC OR SALE PRICE.")

class Invoice(BaseModel):
    """Extract the invoice number, date and all list items with description, quantity and gross worth and the total gross worth."""
    invoice_number: str = Field(description="The invoice number")
    date: str = Field(description="The date of the invoice e.g. 2024-01-01")
    PO_Number: str = Field(description="PO NUMBER (either 10 digits number or 6-character code that starts with the letter P, followed by 6 digits.) e.g. (3100641744 or P999792)")
    items: list[Item] = Field(description="The list of items with description, quantity and gross worth")
    total_gross_worth: float = Field(description="The total gross worth of the invoice")

class Description(BaseModel):
    product_variant_id: str
    description: str
    product_name: str

class PVID(BaseModel):
    """Extract the invoice number, date and all list items with description, quantity and gross worth and the total gross worth."""
    
    items: list[Description] = Field(description="The list of items with product_variant_id, description on invoice  and product_name ")


# Fix the split_pdf function to correctly upload the split PDF to GCS

def split_pdf(gcs_uri, max_pages=4):
    """Splits a PDF stored in GCS into smaller PDFs (also stored in GCS) with a maximum number of pages using PyMuPDF."""
    try:
        # Extract bucket name and blob name from GCS URI
        bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the PDF from GCS into memory
        pdf_data = blob.download_as_bytes()
        doc = fitz.open(stream=io.BytesIO(pdf_data), filetype="pdf")
        num_pages = doc.page_count
        if num_pages <= max_pages:
            return [(gcs_uri, 1, num_pages)]  # No need to split

        split_paths = []
        for start_page in range(0, num_pages, max_pages):
            end_page = min(start_page + max_pages, num_pages)
            new_doc = fitz.open()  # Create a new PDF
            new_doc.insert_pdf(doc, from_page=start_page, to_page=end_page - 1)

            # Upload the split PDF to GCS
            split_blob_name = f"{os.path.splitext(blob_name)[0]}_part{start_page // max_pages + 1}.pdf"
            split_blob = bucket.blob(split_blob_name)

            # Convert the PDF to bytes using to_bytes()
            pdf_bytes = new_doc.tobytes()

            # Upload the bytes using upload_from_file with BytesIO
            split_blob.upload_from_file(io.BytesIO(pdf_bytes), content_type='application/pdf')

            split_gcs_uri = f"gs://{bucket_name}/{split_blob_name}"
            split_paths.append((split_gcs_uri, start_page + 1, end_page))
            new_doc.close()
        doc.close()
        return split_paths
    except Exception as e:
        logging.error(f"Error splitting PDF {gcs_uri}: {e}")
        return [(gcs_uri, 1, 0)]  # Return original PDF path in case of error
    
@retry(max_retries=10, backoff_factor=1, exceptions=(Exception,))
def po_details(PO_NUMBER):
        if len(PO_NUMBER) == 7:
            query = F"""SELECT 
                            LOWER(sku) as product_variant_id, 
                            CONCAT(product_name, ' ', pack_size, ' ', uom) AS product_name 
                        FROM 
                            gold.ops.pl_po_details 
                        WHERE
                            externpocode='{PO_NUMBER}' """
        else:
            query = F"""SELECT 
                            LOWER(a.pvid) AS product_variant_id,
                            CONCAT(product_name, ' ', packsize, ' ', unit_of_measure) AS product_name
                        FROM 
                            silver.sap.all_po_details a
                        JOIN 
                            gold.zepto.sku_info b
                        ON 
                            LOWER(a.pvid) = b.product_variant_id
                        WHERE 
                            a.pono = '{PO_NUMBER}';"""
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        result_dict = {row[0]: row[1] for row in results}
        return result_dict
        #cursor.close()
        #conn.close()

        
        result_dict = {row[0]: row[1] for row in results}
        #product_variants: List[PO] = [ProductVariant(product_variant_id=row[0], product_name=row[1])for row in results]
        #json_output = json.dumps([variant.dict() for variant in product_variants], indent=2)
        return result_dict



@retry(max_retries=10, backoff_factor=1, exceptions=(Exception,))
def extract_structured_data(file_path: str, email_address: str = None, model: BaseModel = Invoice):
    """
    Extracts structured data using Vertex AI. If file_path is already a GCS URI,
    it will be used directly. Otherwise, the file is uploaded to GCS.
    """
    # # If file_path already is a GCS URI, use it directly.
    # if file_path.startswith("gs://"):
    #     file_uri = file_path
    # # else:
    # #     file_ref = upload_file_to_gcs(file_path, email_address)
    # #     file_uri = file_ref["gcsUri"]
        
    prompt = "Extract the structured data from the following PDF file"
    logging.info(f"Extracting structured data from {file_path} using model {model.__name__}")
    #tokens_used = client.models.count_tokens(model=model_id, contents=[prompt, file_path])
    
    pdf_file = Part.from_uri(
                file_uri=file_path,
                mime_type="application/pdf",
                ) 
    tokens_used = client.models.count_tokens(model=model_id, contents=[prompt, pdf_file])  
    logging.info(f"Tokens needed for extract_structured_data: {tokens_used}")
    response = generate_content_with_retry(
         model=model_id,
         contents=[prompt,pdf_file],config={'response_mime_type': 'application/json', 'response_schema': model})
    
    if response is None or response.parsed is None:
        logging.error(f"Failed to generate content for structured data extraction from {file_path}. Response: {response}") # Include response in log
        return None

    result = response.parsed
    return json.loads(result.model_dump_json())
    
def invoice_pdf(pdf_path):
    return client.files.upload(file= pdf_path, config={'display_name': 'invoice'})

def file_size(pdf_path):
    file_size = client.models.count_tokens(model=model_id,contents=invoice_pdf)
    return file_size


@retry(max_retries=10, backoff_factor=1, exceptions=(Exception,))
def string_matching(description,mapping,model=PVID):
    description = [item['description'] for item in description['items']]

    prompt = f"""Act as an expert "description to product_variant_id" matcher and find the best possible product_variant_id for ALL PRODUCT DESCRIPTION provided by the user
            {description}
       matching to 
                 product_variant_id-to-product-name mapping
            Use the following product_variant_id-to-product-name: {str(mapping)}. """
    
    # Count tokens for prompt
    #tokens_used = client.models.count_tokens(model=model_id, contents=[prompt])
    #logging.info(f"Tokens required for string_matching: {tokens_used}")
    
    response = generate_content_with_retry(
        model=model_id,
        contents=[prompt],
        config={'response_mime_type': 'application/json', 'response_schema': model}
    )
    if response is None or response.parsed is None:
        logging.error(f"Failed to generate content for structured data extraction. Response: {response}") # Include response in log
        return None

    result = response.parsed
    return json.loads(result.model_dump_json())

def postprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows with non-sensical data.
    Filters out rows where:
      - PO_Number is null, empty, or one of ['NA', 'N/A', 'unknown']
      - product_variant_id is null, empty, or 'unknown' (case-insensitive)
      - product_name is null, empty, or 'unknown' (case-insensitive)
    """
    # Ensure string type for comparison
    df['PO_Number'] = df['PO_Number'].astype(str).str.strip()
    df['product_variant_id'] = df['product_variant_id'].astype(str).str.strip()
    df['product_name'] = df['product_name'].astype(str).str.strip()
    
    valid_po = ~df['PO_Number'].str.lower().isin(['na', 'n/a', 'unknown', '', 'string'])
    valid_pvid = ~df['product_variant_id'].str.lower().isin(['na', 'n/a', 'unknown', '', 'string'])
    valid_prodname = ~df['product_name'].str.lower().isin(['na', 'n/a', 'unknown', '', 'string'])
    
    filtered_df = df[valid_po & valid_pvid & valid_prodname].copy()
    return filtered_df

def app(pdf_path, start_page=None, end_page=None, global_meta=None):
    try:
        extracted_data = extract_structured_data(pdf_path)
        if extracted_data is None:
            logging.error(f"Failed to extract structured data from {pdf_path} (pages {start_page}-{end_page}).")
            return None

        # If global metadata from the first page exists, override these fields.
        if global_meta is not None:
            extracted_data['invoice_number'] = global_meta.get('invoice_number', extracted_data.get('invoice_number'))
            extracted_data['PO_Number'] = global_meta.get('PO_Number', extracted_data.get('PO_Number'))

        PO_Number = extracted_data['PO_Number']
        logging.info(f"PO_Number: {PO_Number} (pages {start_page}-{end_page})")
        po_detail = po_details(PO_Number)
        pvid = string_matching(extracted_data, po_detail)
        if pvid is None:
            logging.error(f"Failed to perform string matching for {pdf_path} (pages {start_page}-{end_page}).")
            return None

        pvid_df = pd.DataFrame(pvid['items'])
        df = pd.DataFrame(extracted_data['items'])
        df['invoice_number'] = extracted_data['invoice_number']
        df['PO_Number'] = extracted_data['PO_Number']

        # Check if 'description' column exists in both DataFrames
        if 'description' not in pvid_df.columns or 'description' not in df.columns:
            logging.error(f"'description' column missing in one of the dataframes for {pdf_path} (pages {start_page}-{end_page}).")
            return None

        merged_df = pd.merge(pvid_df, df, on='description', how='inner')
        merged_df['date'] = datetime.now().strftime('%d.%m.%Y')
        merged_df = merged_df[['PO_Number','invoice_number','product_variant_id','quantity','date','product_name','description']]
        
        # Add source info column with pdf file name and page range.
        merged_df['source_info'] = f"{os.path.basename(pdf_path)} (pages {start_page}-{end_page})"
        
        return merged_df
    except Exception as e:
        logging.error(f"Error processing {pdf_path} (pages {start_page}-{end_page}): {e}")
        return None


def quality_checks(df: pd.DataFrame, sql_mapping: dict) -> pd.DataFrame:
    """
    Compare the invoice product name (extracted from PDF) with the internal product name from SQL.
    Adds the following columns:
      - invoice_product_name: product name as extracted from the invoice (PDF)
      - internal_product_name: product name from the internal SQL mapping using product_variant_id
      - name_similarity: similarity ratio between invoice and internal names
      - quality_check: "pass" if similarity >= 0.7, otherwise "fail"
    """
    # Use the invoice product name as extracted in the CSV
    df['invoice_product_name'] = df['description']
    
    
    df['internal_product_name'] = df['product_name']
    
    # Compute similarity ratio using difflib.SequenceMatcher
    def similarity(row):
        inv_name = row['invoice_product_name']
        int_name = row['internal_product_name']
        if int_name == "unknown":
            return 0.0
        return SequenceMatcher(None, inv_name.lower(), int_name.lower()).ratio()
    df['name_similarity'] = df.apply(similarity, axis=1)
    
    # Mark quality check pass/fail based on a threshold (e.g., 0.7)
    df['quality_check'] = df['name_similarity'].apply(lambda x: "pass" if x >= 0.5 else "fail")
    
    # Add pdf_source column using the 'source_info' column added in app()
    df['pdf_source'] = df['source_info'] if 'source_info' in df.columns else "unknown"
    
    return df

def asn_creation(df, TRUE_DIR, existing_po_codes ,check_flag=True):
    os.makedirs(TRUE_DIR, exist_ok=True)
    output_paths = []
    df['referenceNumber'] = df.groupby('PO_Number')['invoice_number'].transform(lambda x: pd.factorize(x)[0] + 1)

    for po_no, group_df in df.groupby("PO_Number"):
        out_file = f"{TRUE_DIR}/ASN_{po_no}.csv"
        group_df.to_csv(out_file, index=False)
        output_paths.append(out_file)

    # -----------
    # Additional Merge for nexus-PO and SAP-PO
    # -----------
    p_list = df["PO_Number"].unique().tolist()
    if p_list:
        po_code_string = ", ".join(f"'{p}'" for p in p_list)
        query_nexus = f"""select *
        from (SELECT 
            externpocode as po_code, 
            sku, 
            po_qty, 
            product_name, 
            CAST(FLOOR(MAX(unit_base_cost) OVER (PARTITION BY externpocode, sku) * 100) / 100 AS DECIMAL(10,2)) AS unit_price, 
            CAST(FLOOR(MAX(unit_mrp) OVER (PARTITION BY externpocode, sku) * 100) / 100 AS DECIMAL(10,2)) AS MRP
            FROM gold.ops.pl_po_details
            WHERE externpocode IN ({po_code_string})
        )
        group by all"""
        
        # code to run databricks in python
        cursor = conn.cursor()
        cursor.execute(query_nexus)
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        po_gsheet_df = pd.DataFrame(results, columns=columns)
        
        

    else:
        # No POs to look up, so we can skip the query or create an empty DataFrame
        po_gsheet_df = pd.DataFrame(columns=["po_code","sku","MRP","unit_price","po_qty","product_name"])
        
    
    # -----------
    # Additional Merge for SAP-PO
    # -----------
    query_sap = f"""
            select a.*, b.product_name
            from gold.scratch.sap_pvid a
            left join gold.zepto.sku_info b on a.product_variant_id=b.product_variant_id
            """
    cursor.execute(query_sap)
    results_sap = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    skucode_mapping = pd.DataFrame(results_sap, columns=columns)
    final_df = df.merge(skucode_mapping, left_on="product_variant_id", right_on="product_variant_id", how="left",suffixes=("", "_sql") )

    # Filter final_df to include only PO_Numbers that start with 'P' or '3'
    final_df = final_df[final_df['PO_Number'].str.startswith(('P', '3'))]

    final_output_paths = []
    for po_no, group_df in final_df.groupby("PO_Number"):
        if str(po_no).startswith('P'):
            # Create lowercase columns
            group_df['product_variant_id'] = group_df['product_variant_id'].str.lower()
            po_gsheet_df['sku'] = po_gsheet_df['sku'].str.lower()

            group_final = group_df.merge(
                po_gsheet_df, 
                left_on=["product_variant_id", "PO_Number"],
                right_on=["sku","po_code"],
                how="left",
                suffixes=("", "_sql") 
            )
            group_final = group_final[[
                "referenceNumber","PO_Number","product_variant_id","product_name",
                "MRP","po_qty","unit_price","quantity"
            ]]

            col_map_2 = {
                "referenceNumber": "referenceNumber",
                "PO_Number": "poCode",
                "product_variant_id": "productVariantID",
                "product_name": "productName",
                "MRP": "mrp",
                "po_qty": "poRemainingQuantity",
                "unit_price": "costPrice",
                "Quantity": "quantity"
            }
            group_final.rename(columns=col_map_2, inplace=True)
            group_final.drop_duplicates(inplace=True)

            group_final["quantity"] = group_final["quantity"].astype(int)
            group_final = group_final.groupby([
                "referenceNumber","poCode","productVariantID","productName",
                "mrp","poRemainingQuantity","costPrice"
            ], as_index=False)["quantity"].sum()

            out_file = f"{TRUE_DIR}/ASN_{po_no}.csv"
            group_final.to_csv(out_file, index=False)
            final_output_paths.append(out_file)
        
        elif str(po_no).startswith('3'):
            group_final = group_df[[ "referenceNumber","PO_Number","product_variant_id","product_name","quantity","date","sap_code"]]
            column_mappings = {
                "PO_Number": "PO No",
                "referenceNumber": "Reference No",
                "sap_code": "Customer Material No",
                "productVariantID": "productVariantID",
                "quantity": "Quantity",
                "date": "Delivery Date",
                "product_name": "SKU Description"
            }
            group_final.rename(columns=column_mappings, inplace=True)
            out_file = f"{TRUE_DIR}/ASN_{po_no}.csv"
            group_final.to_csv(out_file, index=False)
            final_output_paths.append(out_file)
        else :
            logging.error(f"Invalid PO Number {po_no} found in the data.")


    # 5b) Now check if any PO code is already in the Output sheet.
    # For each CSV file, extract the PO number from the file name.
    # If the PO code already exists, "dump" the CSV (i.e. copy it to a dump folder)
    # and skip sending email and logging for that PO.
    final_new_paths = []
    dump_folder = os.path.join(TRUE_DIR, "dump")
    os.makedirs(dump_folder, exist_ok=True)
    for csv_path in final_output_paths:
        base_name = os.path.basename(csv_path)
        po_number_match = re.search(r"ASN_(.+)\.csv$", base_name)
        po_number = po_number_match.group(1) if po_number_match else "Unknown"
        if (po_number in existing_po_codes) and (check_flag):
            # dump_file = os.path.join(dump_folder, base_name)
            # shutil.copy(csv_path, dump_file)
            logging.info(f" PO {po_number} already processed")
        else:
            final_new_paths.append(csv_path)
    return final_new_paths


def process_pdf_file_global(args):
    """
    Process a single PDF file stored in GCS.
    Args:
      args: a tuple (pdf_blob_name, email_address) where
            pdf_blob_name is the full blob name of the PDF in GCS,
            email_address is the email address (folder name) associated with the file.
    Returns:
      A tuple (df, local_mapping) where:
         df is a DataFrame created from the PDF file (or None on failure),
         local_mapping is a dictionary built from the PO details for this PDF.
    """
    pdf_blob_name, email_address = args
    # Construct the full GCS URI for the PDF file
    pdf_path = f"gs://{GCS_BUCKET}/{pdf_blob_name}"
    file_dfs = []
    local_mapping = {}
    # Call split_pdf with the GCS URI input
    split_files = split_pdf(pdf_path)
    global_meta = None

    for split_file, start_page, end_page in split_files:
        if global_meta is None and start_page == 1:
            temp_meta = extract_structured_data(split_file, email_address)
            if temp_meta is not None:
                global_meta = {
                    "invoice_number": temp_meta.get("invoice_number"),
                    "PO_Number": temp_meta.get("PO_Number")
                }
                po_number = global_meta.get("PO_Number")
                if po_number:
                    mapping = po_details(po_number)
                    local_mapping.update(mapping)
        df = app(split_file, start_page, end_page, global_meta)
        if df is not None:
            file_dfs.append(df)
        else:
            logging.error(f"No data extracted from {split_file} (pages {start_page}-{end_page}).")
        # No local file cleanup needed since all files remain in GCS.
    if file_dfs:
        return pd.concat(file_dfs, ignore_index=True), local_mapping
    else:
        return None, {}

def process_email(emailAddress: str):
    """
    Process all PDF invoices for a given email folder in GCS.
    Each email folder is represented by the email address (i.e. the folder name under
    GCS_BASE_PATH). PDF files are processed in parallel from GCS and the resulting CSV files
    are saved locally.
    """
    output_dir = "pdf_output"
    os.makedirs(output_dir, exist_ok=True)
    # Construct the GCS prefix (ensure trailing slash)
    gcs_prefix = f"{GCS_BASE_PATH}{emailAddress}/"
    
    all_dfs = []
    sql_mapping = {}
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # List all PDF blobs under the email folder in GCS
    blobs = bucket.list_blobs(prefix=gcs_prefix)
    pdf_blob_names = [blob.name for blob in blobs if blob.name.lower().endswith(".pdf")]
    
    # Prepare arguments for each PDF file: (full blob name, emailAddress)
    args_list = [(pdf_blob_name, emailAddress) for pdf_blob_name in pdf_blob_names]
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.map(process_pdf_file_global, args_list)
    
    for df_result, local_mapping in results:
        if df_result is not None:
            all_dfs.append(df_result)
        sql_mapping.update(local_mapping)

    # # Process PDF files serially (no multiprocessing)
    # for args in args_list:
    #     df_result, local_mapping = process_pdf_file_global(args)
    #     if df_result is not None:
    #         all_dfs.append(df_result)
    #     sql_mapping.update(local_mapping)
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = postprocess_data(final_df)
        existing_po_codes = []  # Replace with actual existing PO codes if available
        email_output_dir = os.path.join(output_dir, emailAddress)
        os.makedirs(email_output_dir, exist_ok=True)
        finalASNpaths = asn_creation(final_df, email_output_dir, existing_po_codes)
        output_file = os.path.join(output_dir, f"{emailAddress}_pdf.csv")
        final_df.to_csv(output_file, index=False)
        logging.info(f"Saved data for {emailAddress} to {output_file}")
        return finalASNpaths
    else:
        logging.warning(f"No matching PDFs found for {emailAddress}")

def process_all_emails():
    """
    Process each email folder stored in GCS serially.
    The folder structure is assumed to be: 
      GCS_BUCKET/GCS_BASE_PATH/{email_address}/{file.pdf}
    For each email address folder, PDF files are processed in parallel from GCS.
    The final CSV output is stored locally.
    """
    output_dir = "pdf_output"
    os.makedirs(output_dir, exist_ok=True)
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # List all blobs under GCS_BASE_PATH and extract the unique email folder names.
    blobs = bucket.list_blobs(prefix=GCS_BASE_PATH)
    email_folders = set()
    for blob in blobs:
        parts = blob.name.split('/')
        # parts[0] should be GCS_BASE_PATH directory (e.g., "gcs_files")
        # parts[1] should be "invoices"
        # parts[2] is the email address folder
        if len(parts) >= 3:
            email_folders.add(parts[2])
    
    for emailAddress in tqdm(email_folders, desc="Processing Emails"):
        finalASNpaths = process_email(emailAddress)
        if finalASNpaths:
            composeAndSendEmail( emailAddress, finalASNpaths, os.path.join(output_dir, emailAddress))
        else:
            logging.warning(f"No ASN paths returned for {emailAddress}, skipping composeAndSendEmail.")

    #conn.close()


def composeAndSendEmail( email_address, final_new_paths, TRUE_DIR):
    from zoneinfo import ZoneInfo
    timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")
    nexus_dfs = []  # List to store DataFrames for POs starting with "P"
    sap_dfs = []    # List to store DataFrames for POs starting with "3"

    for csv_path in final_new_paths:
        base_name = os.path.basename(csv_path)
        # Expecting file names like "ASN_<PO number>.csv"
        po_number_match = re.search(r"ASN_(.+)\.csv$", base_name)
        if po_number_match:
            po_number = po_number_match.group(1)
            # Read the CSV file (assuming all files share a common structure)
            df_temp = pd.read_csv(csv_path, dtype=str)
            if po_number.startswith("P"):
                nexus_dfs.append(df_temp)
            elif po_number.startswith("3"):
                sap_dfs.append(df_temp)

    merged_files = {}  # Dictionary to store group name -> merged CSV file path

    # Merge and write the Nexus group if any files are present
    if nexus_dfs:
        merged_nexus = pd.concat(nexus_dfs, ignore_index=True)
        nexus_file_name = f"{email_address}_Nexus_{timestamp}.csv"
        nexus_file_path = os.path.join(TRUE_DIR, nexus_file_name)
        merged_nexus.to_csv(nexus_file_path, index=False)
        merged_files["Nexus"] = nexus_file_path

    # Merge and write the SAP group if any files are present
    if sap_dfs:
        merged_sap = pd.concat(sap_dfs, ignore_index=True)
        sap_file_name = f"{email_address}_SAP_{timestamp}.csv"
        sap_file_path = os.path.join(TRUE_DIR, sap_file_name)
        merged_sap.to_csv(sap_file_path, index=False)
        merged_files["SAP"] = sap_file_path

    sender_credentials_list = [("asnautomationtool@zeptonow.com", "gmig chyn osfh qenm")
        # ("fallback1@zeptonow.com", "fallback1_password")
        # ("fallback2@zeptonow.com", "fallback2_password")
        ]
    recipient_emails = [email_address, 'roohi.raj@zeptonow.com']


    from zoneinfo import ZoneInfo
    timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M %p")

    for group, file_path in merged_files.items():
        try:
            df = pd.read_csv(file_path, dtype=str)
            # Assumes that the PO number is in a column named "PO No"
            if "PO No" in df.columns:
                po_numbers = df["PO No"].dropna().unique()
                po_numbers_str = ", ".join(po_numbers)
            elif "poCode" in df.columns:
                po_numbers = df["poCode"].dropna().unique()
                po_numbers_str = ", ".join(po_numbers)    
            else:
                po_numbers_str = "PO column not found."

            # Execute the SQL query to find the corresponding manufacturer for the PO numbers
            query_manufacturer = f"""
                SELECT externpocode, vinculum_vendor_name
                FROM gold.ops.pl_po_details
                WHERE externpocode IN ({po_numbers_str})
            """
            cursor = conn.cursor()
            cursor.execute(query_manufacturer)
            manufacturer_results = cursor.fetchall()
            manufacturer_dict = {row[0]: row[1] for row in manufacturer_results}

            # Get the manufacturer names for all PO numbers in the list
            manufacturers = [manufacturer_dict.get(po, "Unknown Manufacturer") for po in po_numbers]
            manufacturer = ", ".join(set(manufacturers))  # Join unique manufacturer names into a single string
            
        except Exception as e:
            po_numbers_str = f"Error reading file for PO numbers: {e}"
            manufacturer = "Unknown Manufacturer"
        
        if group == "Nexus":
            subject = f"ASN for {manufacturer} - Nexus Group - {timestamp}"
            body = (f"Dear Team,\n\n"
                    f"Please find attached the merged ASN file (Nexus) for {manufacturer}.\n\n"
                    f"PO Numbers: {po_numbers_str}\n\n"
                    "Best regards,\nTeam Zepto")
        elif group == "SAP":
            subject = f"ASN for {manufacturer} - SAP Group - {timestamp}"
            body = (f"Dear Team,\n\n"
                    f"Please find attached the merged ASN file (SAP) for {manufacturer}.\n\n"
                    f"PO Numbers: {po_numbers_str}\n\n"
                    "Best regards,\nTeam Zepto")
            
        send_email_with_sender_fallback(recipient_emails, subject, body, file_path, sender_credentials_list)
if __name__ == "__main__":
    process_all_emails()

    #process_all_emails()
    conn.close()
    #conn.close()
    
# result = app("invoice2.pdf")
# result.to_csv('output.csv',index=False)
# print(f"Extracted Invoice: {result.invoice_number} on {result.date} with total gross worth {result.total_gross_worth}")
# for item in result.items:
#     print(f"Item: {item.description} with quantity {item.quantity} and gross worth {item.gross_worth}")
