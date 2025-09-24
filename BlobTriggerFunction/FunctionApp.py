import logging
import azure.functions as func

def main(myblob: func.InputStream):
    logging.info("BlobTriggerFunction fired.")
    logging.info(f"Blob name: {myblob.name}")
    logging.info(f"Blob size: {myblob.length} bytes")

    try:
        content = myblob.read().decode()
        logging.info(f"Blob content preview: {content[:200]}")
        logging.info("Blob decoded successfully.")

        # Your parsing logic here
    except Exception as e:
        logging.error(f"Error during blob processing: {e}")


# Trigger redeploy to fix indexing





