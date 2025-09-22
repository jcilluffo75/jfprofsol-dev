import logging
import azure.functions as func

def main(blob: func.InputStream):
    logging.info(f"Processing blob: {blob.name}, Size: {blob.length} bytes")

    content = blob.read().decode('utf-8')
    
    # TODO: Parse EDI/CSV/Excel content
    # TODO: Insert into SQL tables: edi_835_raw, edi_835_claims, edi_835_services

    logging.info("Blob processed successfully.")


# print("This won't run")


