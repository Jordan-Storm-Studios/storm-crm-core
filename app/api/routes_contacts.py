from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import uuid

from app.db.session import get_db
from app.schemas.contact import ContactCreate

router = APIRouter(prefix="/contacts", tags=["contacts"])


@router.post("")
def create_contact(payload: ContactCreate, db: Session = Depends(get_db)):
    # 1️⃣ Look for an existing manual rowset
    rowset = db.execute(
        text("""
            SELECT rowset_id
            FROM crm_rowsets
            WHERE stage = 'manual'
            ORDER BY created_at DESC
            LIMIT 1
        """)
    ).first()

    # 2️⃣ Create one if it doesn't exist
    if rowset:
        rowset_id = rowset.rowset_id
    else:
        run_id = str(uuid.uuid4())

       artifact_id = str(uuid.uuid4())

rowset_id = db.execute(
    text("""
        INSERT INTO crm_rowsets (
            run_id,
            artifact_id,
            stage,
            schema_version,
            row_count
        )
        VALUES (
            :run_id,
            :artifact_id,
            'manual',
            'CRMRow.v1',
            0
        )
        RETURNING rowset_id
    """),
    {
        "run_id": run_id,
        "artifact_id": artifact_id
    }
).scalar()


    # 3️⃣ Insert the actual contact row
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
                'active',
                :raw_json
            )
        """),
        {
            "rowset_id": rowset_id,
            "raw_json": payload.model_dump_json()
        }
    )

    db.commit()

    return {"status": "ok", "rowset_id": rowset_id}
