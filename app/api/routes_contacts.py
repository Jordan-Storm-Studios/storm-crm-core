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
    payload = {
        "email": contact.email,
        "first_name": contact.first_name,
        "last_name": contact.last_name,
    }

    run_id = str(uuid.uuid4())
    artifact_id = str(uuid.uuid4())
    operation_id = f"manual_{uuid.uuid4().hex[:8]}"

    try:
        # 1️⃣ Intake run
        db.execute(
            text("""
                INSERT INTO intake_runs (
                    run_id,
                    operation_id,
                    correlation_id,
                    source_system,
                    status
                )
                VALUES (
                    :run_id,
                    :operation_id,
                    :correlation_id,
                    'ui_manual',
                    'RECEIVED'
                )
            """),
            {
                "run_id": run_id,
                "operation_id": operation_id,
                "correlation_id": str(uuid.uuid4()),
            }
        )

        # 2️⃣ Intake artifact
        db.execute(
            text("""
                INSERT INTO intake_artifacts (
                    artifact_id,
                    run_id,
                    payload_json,
                    status
                )
                VALUES (
                    :artifact_id,
                    :run_id,
                    :payload_json,
                    'RECEIVED'
                )
            """),
            {
                "artifact_id": artifact_id,
                "run_id": run_id,
                "payload_json": json.dumps(payload),
            }
        )

        # 3️⃣ Rowset
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
                VALUES (
                    :run_id,
                    :artifact_id,
                    'ORIGINAL',
                    'CRMRow.v1',
                    'inline',
                    1
                )
                RETURNING rowset_id
            """),
            {
                "run_id": run_id,
                "artifact_id": artifact_id,
            }
        ).scalar()

        # 4️⃣ CRM row
        db.execute(
            text("""
                INSERT INTO crm_rows (
                    rowset_id,
                    row_index,
                    row_status,
                    raw_json
                )
                VALUES (
                    :rowset_id,
                    0,
                    'OK',
                    :raw_json
                )
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
            "rowset_id": rowset_id,
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/contacts")
def list_contacts(db: Session = Depends(get_db)):
    try:
        rows = db.execute(
            text("""
                SELECT raw_json
                FROM crm_rows
                ORDER BY created_at DESC
                LIMIT 100
            """)
        ).fetchall()

        contacts = []
        for row in rows:
            value = row.raw_json

            # ✅ FIX: raw_json may already be a dict
            if isinstance(value, dict):
                contacts.append(value)
            elif isinstance(value, str):
                contacts.append(json.loads(value))

        return contacts

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
