from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import uuid
import json

from app.db.session import get_db
from app.schemas.contact import ContactCreate

router = APIRouter()


@router.post("/contacts")
def create_contact(contact: ContactCreate, db: Session = Depends(get_db)):
    # Build payload
    payload = {
        "email": contact.email,
        "first_name": contact.first_name,
        "last_name": contact.last_name,
    }

    run_id = str(uuid.uuid4())
    artifact_id = str(uuid.uuid4())

    # 1️⃣ Create rowset (must satisfy ALL constraints)
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
                'ingest',
                'CRMRow.v1',
                'inline',
                0
            )
            RETURNING rowset_id
        """),
        {
            "run_id": run_id,
            "artifact_id": artifact_id,
        }
    ).scalar()

    # 2️⃣ Insert the actual contact row
    row_id = db.execute(
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
                'active',
                :raw_json
            )
            RETURNING row_id
        """),
        {
            "rowset_id": rowset_id,
            "raw_json": json.dumps(payload),
        }
    ).scalar()

    # 3️⃣ Update row count
    db.execute(
        text("""
            UPDATE crm_rowsets
            SET row_count = row_count + 1
            WHERE rowset_id = :rowset_id
        """),
        {"rowset_id": rowset_id}
    )

    db.commit()

    return {
        "status": "ok",
        "row_id": str(row_id),
        "rowset_id": str(rowset_id),
    }

