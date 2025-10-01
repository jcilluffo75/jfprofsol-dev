import os
import logging
import hashlib
from datetime import datetime, timezone

import azure.functions as func
import pyodbc


# ---- Config helpers ---------------------------------------------------------

def _get_sql_conn():
    """
    Opens a new pyodbc connection using the SQLConnectionString app setting.
    Raises with a clear message if missing.
    """
    conn_str = os.getenv("SQLConnectionString")
    if not conn_str:
        raise RuntimeError("App setting 'SQLConnectionString' is not set.")

    # Strongly recommended driver is 18. Your string uses 17; both can work
    # as long as the driver is installed in the Functions image.
    # Example tweak if you later switch:
    # conn_str = conn_str.replace("ODBC Driver 17", "ODBC Driver 18")

    # pyodbc connects lazily; this will actually open the socket
    return pyodbc.connect(conn_str, autocommit=False)


def _ensure_table(cursor):
    """
    Creates a simple landing table if it doesn't exist yet.
    Idempotent and cheap. You can remove this once your table is provisioned.
    """
    cursor.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.name = N'IncomingBlobs' AND s.name = N'dbo'
        )
        BEGIN
            CREATE TABLE dbo.IncomingBlobs (
                Id               INT IDENTITY(1,1) PRIMARY KEY,
                BlobName         NVARCHAR(512)   NOT NULL,
                Container        NVARCHAR(128)   NOT NULL,
                SizeBytes        BIGINT          NOT NULL,
                ContentSHA256    CHAR(64)        NOT NULL,
                InsertedUtc      DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
                RawText          NVARCHAR(MAX)   NULL
            );

            CREATE UNIQUE INDEX UX_IncomingBlobs_ContentSHA256
            ON dbo.IncomingBlobs(ContentSHA256);
        END
        """
    )


def _insert_row(cursor, *, blob_name, container, size_bytes, sha256_hex, raw_text):
    cursor.execute(
        """
        BEGIN TRY
            INSERT INTO dbo.IncomingBlobs
                (BlobName, Container, SizeBytes, ContentSHA256, RawText)
            VALUES
                (?, ?, ?, ?, ?);
        END TRY
        BEGIN CATCH
            -- Ignore duplicates by hash (idempotent); rethrow everything else
            IF ERROR_NUMBER() NOT IN (2601, 2627) -- duplicate key
                THROW;
        END CATCH
        """,
        (blob_name, container, size_bytes, sha256_hex, raw_text),
    )


# ---- Function entrypoint ----------------------------------------------------

def main(inputBlob: func.InputStream):
    start = datetime.now(timezone.utc)
    blob_full_name = inputBlob.name  # "container/blobpath/filename"
    size_bytes = inputBlob.length

    # Parse container + blob name
    parts = blob_full_name.split("/", 1)
    container = parts[0] if parts else ""
    blob_name = parts[1] if len(parts) > 1 else blob_full_name

    logging.warning("=== BlobTriggerFunction STARTED ===")
    logging.warning("Blob: container=%s, name=%s, size=%d bytes", container, blob_name, size_bytes)

    # Read the blob once (stream can be read a single time)
    content_bytes = inputBlob.read()
    sha256_hex = hashlib.sha256(content_bytes).hexdigest()

    # Decode to text; keep going even if there are a few bad chars
    raw_text = content_bytes.decode("utf-8", errors="replace")

    # Persist to SQL
    conn = None
    try:
        conn = _get_sql_conn()
        cursor = conn.cursor()

        # Optional: keep this while you’re iterating; remove after the table exists
        _ensure_table(cursor)

        _insert_row(
            cursor,
            blob_name=blob_name,
            container=container,
            size_bytes=size_bytes,
            sha256_hex=sha256_hex,
            raw_text=raw_text,
        )
        conn.commit()

        elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        logging.info(
            "Inserted blob metadata into SQL (hash=%s). Duration=%dms",
            sha256_hex, elapsed_ms
        )

    except Exception as ex:
        # Roll back and re-raise so the Function runtime can retry on transient failures
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logging.error("Failed to persist blob to SQL: %s", ex, exc_info=True)
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    logging.info("=== BlobTriggerFunction COMPLETED ===")


