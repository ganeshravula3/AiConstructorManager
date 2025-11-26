import os
import uuid
import json
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from di_client import analyze_invoice

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
STORAGE_DIR = BASE_DIR / "backend" / "storage"
BILLS_DIR = STORAGE_DIR / "bills"
BILLS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Construction Bill Verification - Prototype")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/upload_bill")
async def upload_bill(file: UploadFile = File(...), tenant: str = "default", project: str = "proj"):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported in this prototype")
    bill_id = str(uuid.uuid4())
    target_dir = BILLS_DIR / tenant / project
    target_dir.mkdir(parents=True, exist_ok=True)
    file_path = target_dir / f"{bill_id}.pdf"
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    # parse using Azure Document Intelligence (prebuilt invoice)
    try:
        parsed = analyze_invoice(str(file_path))
    except Exception as e:
        # fallback minimal parsed object to avoid breaking callers
        parsed = {"bill_id": bill_id, "error": str(e)}

    parsed_path = STORAGE_DIR / "parsed"
    parsed_path.mkdir(parents=True, exist_ok=True)
    with open(parsed_path / f"{bill_id}.json", "w") as f:
        json.dump(parsed, f, indent=2)

    # In production: insert DB entry, push event to Event Grid
    return JSONResponse({"bill_id": bill_id, "status": "uploaded"})

@app.get("/get_bill_result/{bill_id}")
async def get_bill_result(bill_id: str):
    parsed_path = STORAGE_DIR / "parsed" / f"{bill_id}.json"
    if not parsed_path.exists():
        raise HTTPException(status_code=404, detail="Bill not found")
    with open(parsed_path) as f:
        parsed = json.load(f)
    # stub: load anomaly scores + LLM reasoner if available
    result = {
        "bill_id": bill_id,
        "parsed": parsed,
        "fraud_score": 0.12,
        "fraud_explanation": "Low risk - sample prototype result",
        "status": "analysed"
    }
    return result
