from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/contacts", tags=["contacts"])


@router.post("/")
def create_contact(payload: dict, db: Session = Depends(get_db)):
    """
    Minimal v1 contact creation.
    Expects a JSON body like:
    {
      "email": "test@example.com",
      "first_name": "Jane",
      "last_name": "Doe",
      "job_title": "Head of Growth"
    }
    """

    # NOTE:
    # For now, we just store the raw payload.
    # We will map this properly to crm_rows + fields next.
    # This endpoint is just to prove wiring works.

    return {
        "status": "ok",
        "received": payload
    }


@router.get("/{row_id}")
def get_contact(row_id: str, db: Session = Depends(get_db)):
    """
    Fetch a contact by CRM row_id.
    (Stub for now)
    """

    return {
        "row_id": row_id,
        "status": "stub"
    }

