import pyodbc
import os
import logging
import azure.functions as func

def main(inputBlob: func.InputStream):
    logging.info("BlobTriggerFunction fired.")
    logging.info(f"Blob name: {inputBlob.name}")
    logging.info(f"Blob size: {inputBlob.length} bytes")

    try:
        content = inputBlob.read().decode()
        logging.info(f"Blob content preview: {content[:200]}")
        logging.info("Blob decoded successfully.")

        # Example parsing logic (replace with your actual parser)
        claim_id = "TEST123"
        amount = 100.00

        # Connect to SQL
        try:
            conn_str = os.environ["SQLConnectionString"]
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            logging.info("Connected to SQL database.")
            logging.info(f"Inserting ClaimID: {claim_id}, Amount: {amount}")

            cursor.execute(
                "INSERT INTO dbo.edi_835_claims (ClaimID, Amount) VALUES (?, ?)",
                claim_id, amount
            )
            conn.commit()
            logging.info("SQL insert successful.")

        except Exception as sql_error:
            logging.error(f"SQL insert failed: {sql_error}")

    except Exception as e:
        logging.error(f"Error during blob processing: {e}")





