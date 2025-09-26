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
        if not yyyymmdd: return None
        return datetime.datetime.strptime(yyyymmdd, "%Y%m%d").date()
    except Exception:
        return None

def split_composite(val, comp_sep="^"):
    # e.g. "HC^A0428^RJ" -> ("HC", "A0428", "RJ")
    return tuple(val.split(comp_sep)) if val else tuple()

def main(inputBlob: func.InputStream):
    logging.info("835 ingest started: %s (%d bytes)", inputBlob.name, inputBlob.length)

    # ---------- read + basic tokenize ----------
    content = inputBlob.read().decode(errors="replace")
    segs = [s for s in content.split("~") if s.strip()]  # segment terminator "~"
    # Most lines use "|" as element separator in your sample
    elem_sep = "|"
    comp_sep = "^"

    # ---------- connect to SQL ----------
    conn_str = os.environ["SQLConnectionString"]
    cn = pyodbc.connect(conn_str)
    cn.autocommit = False
    cur = cn.cursor()

    now = datetime.datetime.utcnow()

    # ---------- insert all raw segments ----------
    # dbo.edi_835_raw(id PK identity, segment_type, segment_content, source_file_name, ingestion_timestamp)
    raw_rows = []
    for s in segs:
        stype = s.split(elem_sep, 1)[0].strip()
        raw_rows.append((stype, s, inputBlob.name, now))

    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO dbo.edi_835_raw (segment_type, segment_content, source_file_name, ingestion_timestamp)
        VALUES (?, ?, ?, ?)
    """, raw_rows)

    # ---------- parse to claims + services ----------
    current_claim_db_id = None  # FK for services
    current_claim_ctx = {
        "patient_id": None,  # from NM1*QC (element 9 in your sample line)
        "provider_id": None  # from NM1*74 or PE as needed
    }
    pending_service_id = None  # last inserted service row (to update DTM/CAS)

    for seg in segs:
        parts = seg.split(elem_sep)
        tag = parts[0].strip()

        if tag == "CLP":
            # Reset service context for a new claim
            current_claim_db_id = None
            pending_service_id = None
            # Map CLP elements (X12 835)
            # CLP01 submitter's claim ID; CLP02 status; CLP03 total; CLP04 payment; CLP05 patient resp;
            # CLP07 payer claim control; CLP08 facility type; CLP09 freq code
            clp_patient_control_number = parts[1].strip() if len(parts) > 1 else None
            clp_claim_status_code      = parts[2].strip() if len(parts) > 2 else None
            clp_total_charge_amount    = dec(parts[3].strip() if len(parts) > 3 else None)
            clp_payment_amount         = dec(parts[4].strip() if len(parts) > 4 else None)
            clp_patient_responsibility = dec(parts[5].strip() if len(parts) > 5 else None)
            clp_payer_claim_ctrl_num   = parts[7].strip() if len(parts) > 7 else None
            clp_facility_type_code     = parts[8].strip() if len(parts) > 8 else None
            clp_claim_frequency_code   = parts[9].strip() if len(parts) > 9 else None

            # DTM service date often appears after NM1 segments; we’ll capture later when we see DTM*050/472
            dtm_service_date = None

            # Insert claim row
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
            logging.info("Inserted claim_id=%s (CLP01=%s)", current_claim_db_id, clp_patient_control_number)

        elif tag == "NM1":
            # Patient (QC) or Provider (74/PE) — in your sample, patient ID appears in element 9
            # Example: NM1|QC|1|AYALA|ISRAEL||||MI|B7U3HZN37292890
            entity = parts[1].strip() if len(parts) > 1 else None
            id_qual = parts[8].strip() if len(parts) > 8 else None
            id_val  = parts[9].strip() if len(parts) > 9 else None
            if entity == "QC":
                current_claim_ctx["patient_id"] = id_val
            elif entity in ("74","PE"):
                current_claim_ctx["provider_id"] = id_val

            # If a claim row was already inserted for this CLP, update patient/provider IDs
            if current_claim_db_id is not None and (entity in ("QC","74","PE")):
                cur.execute("""
                    UPDATE dbo.edi_835_claims
                    SET nm1_patient_id = COALESCE(?, nm1_patient_id),
                        nm1_provider_id = COALESCE(?, nm1_provider_id)
                    WHERE claim_id = ?
                """,
                current_claim_ctx["patient_id"], current_claim_ctx["provider_id"], current_claim_db_id)

        elif tag == "DTM":
            # DTM*472 (service date) for SVC; DTM*050 (received), *232/*233 (admission/discharge)
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
            # SVC|HC^A0428^RJ|<charge>|<paid>|... pattern; we’ll capture charge/paid and the procedure composite
            proc_composite = parts[1].strip() if len(parts) > 1 else None
            charge_amt     = dec(parts[2].strip() if len(parts) > 2 else None)
            paid_amt       = dec(parts[3].strip() if len(parts) > 3 else None)
            # explode "HC^A0428^RJ"
            pc = split_composite(proc_composite, comp_sep)
            svc_procedure_code = None
            if len(pc) >= 2:
                # take the code portion (e.g., A0428). If you want the qualifier/modifier, store them too.
                svc_procedure_code = pc[1]

            cur.execute("""
                INSERT INTO dbo.edi_835_services
                    (claim_id, svc_procedure_code, svc_charge_amount, svc_paid_amount)
                OUTPUT INSERTED.service_id
                VALUES (?, ?, ?, ?)
            """,
            current_claim_db_id, svc_procedure_code, charge_amt, paid_amt)
            pending_service_id = cur.fetchone()[0]

        elif tag == "CAS" and pending_service_id is not None:
            # CAS|<group>|<reason>|<amount>|...  (we’ll record first triple)
            cas_group  = parts[1].strip() if len(parts) > 1 else None
            cas_reason = parts[2].strip() if len(parts) > 2 else None
            cas_amt    = dec(parts[3].strip() if len(parts) > 3 else None)
            cur.execute("""
                UPDATE dbo.edi_835_services
                   SET cas_group_code = ?, cas_reason_code = ?, cas_adjustment_amount = ?
                 WHERE service_id = ?
            """, cas_group, cas_reason, cas_amt, pending_service_id)

        # (Add any additional segments you care about; REF 6R is on your sample and can be stored too.)

    cn.commit()
    logging.info("835 ingest complete for %s", inputBlob.name)






