from fastapi import APIRouter, Depends, HTTPException, Query
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
def list_contacts(
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    rows = db.execute(
        text("""
            SELECT row_id, raw_json
            FROM crm_rows
            ORDER BY created_at DESC
            LIMIT 100
        """),
        {"limit": limit, "offset": offset}
    ).fetchall()

    results = []
    for row in rows:
        contact_data = row.raw_json
        if isinstance(contact_data, str):
            contact_data = json.loads(contact_data)

        results.append({
            "row_id": str(row.row_id),
            "email": contact_data.get("email"),
            "first_name": contact_data.get("first_name"),
            "last_name": contact_data.get("last_name")
        })
            
    return results


# ===============================
# GET /contacts/{row_id}
# ===============================
@router.get("/contacts/{row_id}")
def get_contact(row_id: str, db: Session = Depends(get_db)):
    result = db.execute(
        text("""
            SELECT row_id, raw_json
            FROM crm_rows
            WHERE row_id = CAST(:row_id AS UUID)
            LIMIT 1
        """),
        {"row_id": row_id}
    ).fetchone()

    if result is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Contact with ID {row_id} not found in crm_rows"
        )

    contact_data = result.raw_json
    if isinstance(contact_data, str):
        contact_data = json.loads(contact_data)
    
    # Optional: include the DB id in the response for debugging
    contact_data["row_id"] = str(result.row_id)
    
    return contact_data
