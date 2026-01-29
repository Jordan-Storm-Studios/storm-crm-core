from fastapi import FastAPI

app = FastAPI(title="Storm CRM Core")

@app.get("/health")
def health():
    return {"status": "ok"}

