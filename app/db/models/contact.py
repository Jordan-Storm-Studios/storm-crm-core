from sqlalchemy import Column, String, Boolean, UUID
from app.db.base import Base

class Contact(Base):
    __tablename__ = "contacts"

    id = Column(UUID, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String, unique=True, index=True)
    status = Column(String)
    suppressed = Column(Boolean)

