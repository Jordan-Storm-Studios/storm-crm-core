from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.schemas.contact import ContactCreate

router = APIRouter(prefix="/contacts", tags=["contacts"])

@router.post("")
def create_contact(payload: ContactCreate, db: Session = Depends(get_db)):
    # 1. Create a crm_row
    row = db.execute(
        text("""
            INSERT INTO crm_rows (schema_version)
            VALUES ('CRMRow.v1')
            RETURNING id
        """)
    ).first()

    row_id = row.id

    # 2. Get column ids
    columns = db.execute(
        text("""
            SELECT id, column_key
            FROM crm_columns
            WHERE schema_version = 'CRMRow.v1'
              AND column_key IN ('email', 'first_name', 'last_name')
        """)
    ).all()

    column_map = {c.column_key: c.id for c in columns}

    # 3. Insert field values
    for key, value in payload.dict().items():
        if value is None:
            continue

        db.execute(
            text("""
                INSERT INTO crm_field_values (row_id, column_id, value)
                VALUES (:row_id, :column_id, :value)
            """),
            {
                "row_id": row_id,
                "column_id": column_map[key],
                "value": value
            }
        )

    db.commit()

    return {"id": row_id, "status": "created"}
