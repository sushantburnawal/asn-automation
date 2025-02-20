from pdfParse import process_all_emails
from invoiceExtraction import getInvoices

if __name__ == "__main__":
    getInvoices()
    process_all_emails()
    