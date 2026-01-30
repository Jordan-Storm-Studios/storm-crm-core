from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db.session import get_db
from app.schemas.contact import ContactCreate

router = APIRouter(prefix="/contacts", tags=["contacts"])

@router.post("")
def create_contact(payload: ContactCreate, db: Session = Depends(get_db)):
    # 1️⃣ Ensure a rowset exists (reuse latest one)
    rowset = db.execute(
        text("""
            SELECT rowset_id
            FROM crm_rowsets
            WHERE schema_version = 'CRMRow.v1'
            ORDER BY rowset_id DESC
            LIMIT 1
        """)
    ).first()

    if rowset:
        rowset_id = rowset.rowset_id
    else:
        rowset_id = db.execute(
            text("""
                INSERT INTO crm_rowsets (schema_version, stage)
                VALUES ('CRMRow.v1', 'manual')
                RETURNING rowset_id
            """)
        ).scalar()

    # 2️⃣ Insert the contact as a CRM row
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
