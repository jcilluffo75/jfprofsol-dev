import logging
import azure.functions as func

def main(inputBlob: func.InputStream):
    logging.warning("=== BlobTriggerFunction STARTED ===")
    logging.warning("Blob name: %s, size=%d bytes", inputBlob.name, inputBlob.length)
