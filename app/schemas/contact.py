from pydantic import BaseModel, EmailStr
from uuid import UUID

class ContactCreate(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr

class ContactRead(ContactCreate):
    id: UUID

    class Config:
        from_attributes = True

