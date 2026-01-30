from pydantic import BaseModel, EmailStr
from uuid import UUID

class ContactCreate(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    email: EmailStr

class ContactRead(ContactCreate):
    id: UUID
    status: str
    suppressed: bool

    class Config:
        from_attributes = True

