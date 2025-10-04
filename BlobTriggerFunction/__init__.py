import logging
import azure.functions as func
import pyodbc

def parse_edi(edi_text):
    # Placeholder: parse EDI segments and return structured claims
    logging.info("Starting EDI parsing")
    claims = []
    for line in edi_text.split("~"):
        if line.startswith("CLP"):
            parts = line.split("|")
            claim_id = parts[1]
            amount = float(parts[3])
            claims.append({"claim_id": claim_id, "amount": amount})
    logging.info(f"Parsed {len(claims)} claims")
    return claims

def insert_claims_to_sql(claims):
    try:
        conn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=tcp:sql-jfprofsol-dev.database.windows.net,1433;"
            "Database=JFProfSol-dev-db;"
            "Uid=jfprofsol_adm;"
            "Pwd=kBL&SUZ.>M(m@-<@c;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        logging.info("Connected to SQL successfully")

        for claim in claims:
            cursor.execute(
                "INSERT INTO dbo.BlobAudit (ClaimID, Amount) VALUES (?, ?)",
                claim["claim_id"], claim["amount"]
            )
        conn.commit()
        logging.info(f"Inserted {len(claims)} rows into BlobAudit")
        return True

    except Exception as e:
        logging.error(f"SQL insert failed: {str(e)}", exc_info=True)
        return False

def main(inputBlob: func.InputStream):
    logging.warning("=== Blob Trigger Activated ===")
    try:
        edi_content = inputBlob.read().decode('utf-8')
        logging.info(f"EDI file size: {len(edi_content)} bytes")

        claims = parse_edi(edi_content)
        success = insert_claims_to_sql(claims)

        if success:
            logging.info("Function completed successfully")
        else:
            logging.warning("Function completed with SQL insert failure")

    except Exception as e:
        logging.error(f"Unhandled exception: {str(e)}", exc_info=True)
