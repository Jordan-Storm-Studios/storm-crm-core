from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import uuid
import json

from app.db.session import get_db
from app.schemas.contact import ContactCreate

router = APIRouter()

@router.post("/contacts")
def create_contact(contact: ContactCreate, db: Session = Depends(get_db)):
    # 0️⃣ Prepare data and IDs
    payload = {
        "email": contact.email,
        "first_name": contact.first_name,
        "last_name": contact.last_name,
    }
    
    # We must generate IDs manually if we want to use them across related inserts
    run_id = str(uuid.uuid4())
    artifact_id = str(uuid.uuid4())
    operation_id = f"manual_{uuid.uuid4().hex[:8]}"

    try:
        # 1️⃣ Create the Intake Run (Required for Foreign Key)
        # Schema: operation_id, correlation_id, and source_system are NOT NULL [cite: 14, 15, 16]
        db.execute(
            text("""
                INSERT INTO intake_runs (run_id, operation_id, correlation_id, source_system, status)
                VALUES (:run_id, :op_id, :corr_id, 'ui_manual', 'RECEIVED')
            """),
            {
                "run_id": run_id,
                "op_id": operation_id,
                "corr_id": str(uuid.uuid4()),
            }
        )

        # 2️⃣ Create the Intake Artifact (Required for Foreign Key)
        # Schema: run_id and payload_json are required [cite: 44, 47]
        db.execute(
            text("""
                INSERT INTO intake_artifacts (artifact_id, run_id, payload_json, status)
                VALUES (:artifact_id, :run_id, :payload, 'RECEIVED')
            """),
            {
                "artifact_id": artifact_id,
                "run_id": run_id,
                "payload": json.dumps(payload)
            }
        )

        # 3️⃣ Create the Rowset
        # FIX: 'raw' is changed to 'ORIGINAL' to satisfy CHECK constraint 
        rowset_id = db.execute(
            text("""
                INSERT INTO crm_rowsets (
                    run_id, 
                    artifact_id, 
                    stage, 
                    schema_version, 
                    storage_uri, 
                    row_count
                )
                VALUES (:run_id, :artifact_id, 'ORIGINAL', 'CRMRow.v1', 'inline', 1)
                RETURNING rowset_id
            """),
            {"run_id": run_id, "artifact_id": artifact_id}
        ).scalar()

        # 4️⃣ Insert the CRM Row
        # FIX: 'active' is changed to 'OK' or 'PENDING' to satisfy CHECK constraint 
        row_id = db.execute(
            text("""
                INSERT INTO crm_rows (
                    rowset_id, 
                    row_index, 
                    row_status, 
                    raw_json
                )
                VALUES (:rowset_id, 0, 'OK', :raw_json)
                RETURNING row_id
            """),
            {
                "rowset_id": rowset_id,
                "raw_json": json.dumps(payload),
            }
        )

        db.commit()

        return {
            "status": "ok",
            "run_id": run_id,
            "artifact_id": artifact_id,
            "rowset_id": rowset_id
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
