import logging
import azure.functions as func
import os
import pyodbc

def main(inputBlob: func.InputStream):
    logging.warning("=== BlobTriggerFunction STARTED ===")
    logging.warning("Blob name: %s, size=%d bytes", inputBlob.name, inputBlob.length)

    try:
        # Get SQL connection string from App Settings
        conn_str = os.environ["SQLConnectionString"]
        logging.info("Connecting to SQL...")

        # Connect
        cn = pyodbc.connect(conn_str, timeout=15)
        cur = cn.cursor()
        logging.info("Connected to SQL successfully.")

        # Just for proof: insert a log row into a test table
        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BlobAudit' AND xtype='U')
            CREATE TABLE BlobAudit (
                Id INT IDENTITY PRIMARY KEY,
                BlobName NVARCHAR(500),
                BlobSize BIGINT,
                CreatedAt DATETIME DEFAULT GETDATE()
            )
        """)
        cn.commit()

        cur.execute(
            "INSERT INTO BlobAudit (BlobName, BlobSize) VALUES (?, ?)",
            (inputBlob.name, inputBlob.length)
        )
        cn.commit()
        logging.info("Inserted audit row into BlobAudit table.")

        # Optional: read back count
        cur.execute("SELECT COUNT(*) FROM BlobAudit")
        count = cur.fetchone()[0]
        logging.info("BlobAudit row count now: %d", count)

    except Exception as e:
        logging.error("SQL operation failed: %s", str(e))
        raise
