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

    # Attempt to validate GSTIN (if present) and include result in the validations
    gstin = parsed.get("vendor_gstin") or parsed.get("gstin") or parsed.get("tax_id")
    vendor = parsed.get("vendor") or parsed.get("supplier") or parsed.get("Vendor")
    vendor_name = None
    if isinstance(vendor, dict):
        vendor_name = vendor.get("name") or vendor.get("vendor_name") or vendor.get("VendorName")
        gstin = gstin or vendor.get("gstin")
    elif isinstance(vendor, str):
        vendor_name = vendor

    gstin_validation = None
    if gstin or vendor_name:
        try:
            from .validation import validate_gstin
            gstin_validation = validate_gstin(gstin or "", vendor_name=vendor_name)
        except Exception as e:
            gstin_validation = {"error": str(e)}

    # attach GSTIN validation to validations for frontend visibility
    validations["gstin_validation"] = gstin_validation

    # Simple heuristic-based fraud scoring (0.0 low risk -> 1.0 high risk)
    score = 0.0
    reasons = []

    # 1) invoice total mismatch is a strong signal
    if sum_ok is False and sum_diff is not None:
        if invoice_total and invoice_total != 0:
            ratio = min(abs(sum_diff) / abs(invoice_total), 1.0)
            add = 0.5 * ratio
        else:
            add = 0.5
        score += add
        reasons.append(f"Invoice total differs from sum of lines by {sum_diff}")

    # 2) per-line discrepancies add smaller incremental risk
    line_issues = sum(1 for l in line_checks if l.get("ok") is False)
    if line_issues:
        add = min(0.25, 0.05 * line_issues)
        score += add
        reasons.append(f"{line_issues} line item(s) with mismatched totals")

    # 3) GSTIN-based signals
    if gstin_validation:
        # business_name_match is provided by validate_gstin when vendor_name supplied
        bn_match = None
        if isinstance(gstin_validation, dict):
            bn_match = gstin_validation.get("business_name_match")
            # look for an indication the GSTIN was found in registry
            external = gstin_validation.get("external_check")
            # common field names for format check
            format_ok = gstin_validation.get("format_ok") or gstin_validation.get("valid_format") or gstin_validation.get("is_valid")

            if format_ok is False:
                score += 0.15
                reasons.append("GSTIN format appears invalid")

            if external and isinstance(external, dict):
                # registry response may contain a name or status; treat absence as suspicious
                if external.get("status") in ("not_found", "not_exists") or not external.get("business_name"):
                    score += 0.35
                    reasons.append("GSTIN not found in external registry")

            if bn_match is False:
                score += 0.30
                reasons.append("Vendor name does not match registry/business name for GSTIN")
            elif bn_match is True:
                # small negative contribution for positive match (reduces risk a little)
                score = max(0.0, score - 0.05)
                reasons.append("Vendor name matches registry for GSTIN")
    else:
        # no GSTIN information at all increases risk modestly
        if not gstin:
            score += 0.10
            reasons.append("No GSTIN provided")

    # clamp and round score
    if score < 0:
        score = 0.0
    if score > 1:
        score = 1.0
    fraud_score = round(score, 2)

    if reasons:
        fraud_explanation = "; ".join(reasons)
    else:
        fraud_explanation = "Low risk - no significant arithmetic or GSTIN issues detected"

    result = {
        "bill_id": bill_id,
        "parsed": parsed,
        "validations": validations,
        "fraud_score": fraud_score,
        "fraud_explanation": fraud_explanation,
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