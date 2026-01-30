from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import uuid
import json

from app.db.session import get_db
from app.schemas.contact import ContactCreate

router = APIRouter()


# ===============================
# POST /contacts
# ===============================
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

        # 3️⃣ CRM rowset
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


# ===============================
# GET /contacts
# ===============================
@router.get("/contacts")
def list_contacts(db: Session = Depends(get_db)):
    rows = db.execute(
        text("""
            SELECT raw_json
            FROM crm_rows
            ORDER BY created_at DESC
            LIMIT 100
        """)
    ).fetchall()

    return [json.loads(row.raw_json) for row in rows]


# ===============================
# GET /contacts/{row_id}
# ===============================
@router.get("/contacts/{row_id}")
def get_contact(row_id: str, db: Session = Depends(get_db)):
    # 1️⃣ We cast :row_id to UUID explicitly to ensure Postgres matches the type
    # 2️⃣ We fetch row_id as well to confirm identity
    result = db.execute(
        text("""
            SELECT row_id, raw_json
            FROM crm_rows
            WHERE row_id = :row_id::uuid
            LIMIT 1
        """),
        {"row_id": row_id}
    ).fetchone()

    # 3️⃣ 404 Handling
    if result is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Contact with ID {row_id} not found in crm_rows"
        )

    # 4️⃣ Parse and return
    contact_data = json.loads(result.raw_json)
    
    # Optional: include the DB id in the response for debugging
    contact_data["row_id"] = str(result.row_id)
    
    return contact_data
