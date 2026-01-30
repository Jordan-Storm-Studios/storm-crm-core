from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models.contact import Contact
from app.schemas.contact import ContactCreate, ContactRead

router = APIRouter(prefix="/contacts", tags=["contacts"])


@router.post("", response_model=ContactRead)
def create_contact(payload: ContactCreate, db: Session = Depends(get_db)):
    existing = (
        db.query(Contact)
        .filter(Contact.email == payload.email)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Contact already exists")

    contact = Contact(
        first_name=payload.first_name,
        last_name=payload.last_name,
        email=payload.email,
    )

    db.add(contact)
    db.commit()
    db.refresh(contact)

    return contact


@router.get("", response_model=list[ContactRead])
def list_contacts(db: Session = Depends(get_db)):
    return db.query(Contact).limit(50).all()

