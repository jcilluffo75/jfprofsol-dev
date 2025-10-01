import os, logging, datetime, pyodbc
import azure.functions as func
from decimal import Decimal, InvalidOperation

# Helpers
def dec(v):
    try:
        return None if v in (None, "",) else Decimal(v)
    except InvalidOperation:
        return None

def safe_date(yyyymmdd):
    try:
        if not yyyymmdd:
            return None
        return datetime.datetime.strptime(yyyymmdd, "%Y%m%d").date()
    except Exception:
        return None

def split_composite(val, comp_sep="^"):
    return tuple(val.split(comp_sep)) if val else tuple()

def main(inputBlob: func.InputStream):
    logging.info("=== 835 ingest started for %s (%d bytes) ===", inputBlob.name, inputBlob.length)

    cn = None
    try:
        # ---------- read + tokenize ----------
        content = inputBlob.read().decode(errors="replace")
        segs = [s for s in content.split("~") if s.strip()]
        elem_sep = "|"
        comp_sep = "^"
        logging.info("Parsed %d segments from blob", len(segs))

        # ---------- connect to SQL ----------
        conn_str = os.environ.get("SQLConnectionString")
        if not conn_str:
            logging.error("❌ SQLConnectionString not found in App Settings")
            return

        logging.info("Connecting to SQL...")
        cn = pyodbc.connect(conn_str, timeout=15)
        cn.autocommit = False
        cur = cn.cursor()
        logging.info("✅ Connected to SQL.")

        # Prove connectivity
        cur.execute("SELECT @@VERSION;")
        logging.info("SQL version: %s", cur.fetchone()[0][:80])

        now = datetime.datetime.utcnow()

        # ---------- insert raw ----------
        raw_rows = []
        for s in segs:
            stype = s.split(elem_sep, 1)[0].strip()
            raw_rows.append((stype, s, inputBlob.name, now))

        if raw_rows:
            cur.fast_executemany = True
            cur.executemany("""
                INSERT INTO dbo.edi_835_raw
                    (segment_type, segment_content, source_file_name, ingestion_timestamp)
                VALUES (?, ?, ?, ?)
            """, raw_rows)
            logging.info("Inserted %d raw rows into edi_835_raw", len(raw_rows))

        # ---------- parse claims + services ----------
        current_claim_db_id = None
        current_claim_ctx = {"patient_id": None, "provider_id": None}
        pending_service_id = None
        claims_added = 0
        services_added = 0

        for seg in segs:
            parts = seg.split(elem_sep)
            tag = parts[0].strip()

            if tag == "CLP":
                current_claim_db_id = None
                pending_service_id = None
                clp_patient_control_number = parts[1].strip() if len(parts) > 1 else None
                clp_claim_status_code = parts[2].strip() if len(parts) > 2 else None
                clp_total_charge_amount = dec(parts[3].strip() if len(parts) > 3 else None)
                clp_payment_amount = dec(parts[4].strip() if len(parts) > 4 else None)
                clp_patient_responsibility = dec(parts[5].strip() if len(parts) > 5 else None)
                clp_payer_claim_ctrl_num = parts[7].strip() if len(parts) > 7 else None
                clp_facility_type_code = parts[8].strip() if len(parts) > 8 else None
                clp_claim_frequency_code = parts[9].strip() if len(parts) > 9 else None
                dtm_service_date = None

                cur.execute("""
                    INSERT INTO dbo.edi_835_claims
                        (clp_patient_control_number, clp_claim_status_code, clp_total_charge_amount,
                         clp_payment_amount, clp_patient_responsibility, clp_payer_claim_control_number,
                         clp_facility_type_code, clp_claim_frequency_code,
                         nm1_patient_id, nm1_provider_id, dtm_service_date,
                         source_file_name, ingestion_timestamp)
                    OUTPUT INSERTED.claim_id
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                clp_patient_control_number, clp_claim_status_code, clp_total_charge_amount,
                clp_payment_amount, clp_patient_responsibility, clp_payer_claim_ctrl_num,
                clp_facility_type_code, clp_claim_frequency_code,
                current_claim_ctx["patient_id"], current_claim_ctx["provider_id"], dtm_service_date,
                inputBlob.name, now)

                current_claim_db_id = cur.fetchone()[0]
                claims_added += 1
                logging.info("Inserted claim_id=%s (CLP01=%s)", current_claim_db_id, clp_patient_control_number)

            elif tag == "NM1":
                entity = parts[1].strip() if len(parts) > 1 else None
                id_val  = parts[9].strip() if len(parts) > 9 else None
                if entity == "QC":
                    current_claim_ctx["patient_id"] = id_val
                elif entity in ("74","PE"):
                    current_claim_ctx["provider_id"] = id_val
                if current_claim_db_id is not None and entity in ("QC","74","PE"):
                    cur.execute("""
                        UPDATE dbo.edi_835_claims
                        SET nm1_patient_id = COALESCE(?, nm1_patient_id),
                            nm1_provider_id = COALESCE(?, nm1_provider_id)
                        WHERE claim_id = ?
                    """,
                    current_claim_ctx["patient_id"], current_claim_ctx["provider_id"], current_claim_db_id)

            elif tag == "DTM":
                if len(parts) > 2:
                    dtm_qual = parts[1].strip()
                    dtm_val  = parts[2].strip()
                    dt = safe_date(dtm_val)
                    if dtm_qual == "472" and pending_service_id is not None:
                        cur.execute("UPDATE dbo.edi_835_services SET service_date = ? WHERE service_id = ?",
                                    dt, pending_service_id)
                    elif dtm_qual in ("050","232","233") and current_claim_db_id is not None:
                        cur.execute("UPDATE dbo.edi_835_claims SET dtm_service_date = ? WHERE claim_id = ?",
                                    dt, current_claim_db_id)

            elif tag == "SVC":
                proc_composite = parts[1].strip() if len(parts) > 1 else None
                charge_amt = dec(parts[2].strip() if len(parts) > 2 else None)
                paid_amt   = dec(parts[3].strip() if len(parts) > 3 else None)
                pc = split_composite(proc_composite, comp_sep)
                svc_procedure_code = pc[1] if len(pc) >= 2 else None

                cur.execute("""
                    INSERT INTO dbo.edi_835_services
                        (claim_id, svc_procedure_code, svc_charge_amount, svc_paid_amount)
                    OUTPUT INSERTED.service_id
                    VALUES (?, ?, ?, ?)
                """,
                current_claim_db_id, svc_procedure_code, charge_amt, paid_amt)
                pending_service_id = cur.fetchone()[0]
                services_added += 1
                logging.info("Inserted service_id=%s (claim_id=%s)", pending_service_id, current_claim_db_id)

            elif tag == "CAS" and pending_service_id is not None:
                cas_group  = parts[1].strip() if len(parts) > 1 else None
                cas_reason = parts[2].strip() if len(parts) > 2 else None
                cas_amt    = dec(parts[3].strip() if len(parts) > 3 else None)
                cur.execute("""
                    UPDATE dbo.edi_835_services
                       SET cas_group_code = ?, cas_reason_code = ?, cas_adjustment_amount = ?
                     WHERE service_id = ?
                """, cas_group, cas_reason, cas_amt, pending_service_id)

        # ---------- commit ----------
        cn.commit()
        logging.info("✅ Commit successful. Added %d claims, %d services, %d raw rows",
                     claims_added, services_added, len(raw_rows))
        logging.info("=== 835 ingest complete for %s ===", inputBlob.name)

    except pyodbc.Error as e:
        logging.error("❌ pyodbc.Error: %s | args=%s", str(e), getattr(e, "args", None), exc_info=True)
        raise
    except Exception as ex:
        logging.error("❌ General error: %s", repr(ex), exc_info=True)
        raise
    finally:
        if cn:
            try:
                cn.close()
                logging.info("SQL connection closed.")
            except Exception:
                pass
