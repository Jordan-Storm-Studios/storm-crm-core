from fastapi import FastAPI

from app.api.routes_contacts import router as contacts_router

app = FastAPI(title="Storm CRM Core")


@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(contacts_router)


