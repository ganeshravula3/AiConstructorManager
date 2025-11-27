import os
import uuid
import json
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from .di_client import analyze_invoice

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
        # Some fields returned by Document Intelligence may be date/datetime objects
        # which are not JSON serializable by default. Use `default=str` to
        # convert such objects to ISO strings when saving parsed output.
        json.dump(parsed, f, indent=2, default=str)

    # In production: insert DB entry, push event to Event Grid
    return JSONResponse({"bill_id": bill_id, "status": "uploaded"})

@app.get("/get_bill_result/{bill_id}")
async def get_bill_result(bill_id: str):
    parsed_path = STORAGE_DIR / "parsed" / f"{bill_id}.json"
    if not parsed_path.exists():
        raise HTTPException(status_code=404, detail="Bill not found")
    with open(parsed_path) as f:
        parsed = json.load(f)
    # Perform arithmetic validations: per-line multiplication and sum of lines
    def _to_number(v):
        if v is None:
            return None
        # handle numbers that may be strings with commas
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

    line_checks = []
    line_items = parsed.get("line_items") or []
    sum_of_line_totals = 0.0
    for idx, li in enumerate(line_items):
        qty = _to_number(li.get("qty") or li.get("quantity"))
        rate = _to_number(li.get("rate") or li.get("unit_price") or li.get("price"))
        total = _to_number(li.get("total") or li.get("amount") or li.get("total_price"))

        computed = None
        ok = None
        diff = None
        if qty is not None and rate is not None:
            computed = round(qty * rate, 2)
            if total is not None:
                diff = round(computed - total, 2)
                ok = abs(diff) <= 1.0  # tolerance: 1 currency unit
        # if total provided use it for sum, otherwise fall back to computed
        sum_of_line_totals += total if total is not None else (computed or 0.0)

        line_checks.append({
            "line_index": idx,
            "item": li.get("item") or li.get("description"),
            "qty": qty,
            "rate": rate,
            "total": total,
            "computed_total": computed,
            "diff": diff,
            "ok": ok,
        })

    invoice_total = _to_number(parsed.get("total_amount") or parsed.get("InvoiceTotal") or parsed.get("amount_due"))
    sum_diff = None
    sum_ok = None
    if invoice_total is not None:
        sum_diff = round(sum_of_line_totals - invoice_total, 2)
        sum_ok = abs(sum_diff) <= 1.0

    validations = {
        "lines": line_checks,
        "sum_of_line_totals": round(sum_of_line_totals, 2),
        "invoice_total": invoice_total,
        "sum_diff": sum_diff,
        "sum_ok": sum_ok,
    }

    result = {
        "bill_id": bill_id,
        "parsed": parsed,
        "validations": validations,
        "fraud_score": 0.12,
        "fraud_explanation": "Low risk - sample prototype result",
        "status": "analysed"
    }
    return result

async def validate_gstin_endpoint(gstin: str, vendor_name: str | None = None):
    from .validation import validate_gstin
    result = validate_gstin(gstin, vendor_name=vendor_name)
    return {
        "gstin": gstin,
        "vendor_name": vendor_name,
        "validation_result": result
    }