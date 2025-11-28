"""Validation helpers: OCR extraction and GSTIN validation.

Requirements (local prototype):
- install poppler (macOS: `brew install poppler`)
- `pip install pytesseract pdf2image Pillow`

This module provides:
- ocr_extract_text_from_pdf(pdf_path) -> str
- find_multiplications_in_text(text) -> dict
- validate_gstin(gstin) -> dict
"""
from pathlib import Path
import re
from typing import List, Dict, Any, Optional
import difflib
import os
from dotenv import load_dotenv
load_dotenv()
import requests
try:
    from pdf2image import convert_from_path
    import pytesseract
    from PIL import Image
except Exception:
    # If OCR deps not installed, functions will raise helpful errors at runtime
    convert_from_path = None
    pytesseract = None


def ocr_extract_text_from_pdf(pdf_path: str, dpi: int = 200) -> str:
    """Extract text from a PDF using pdf2image + pytesseract.

    Returns the concatenated text of all pages.
    """
    if convert_from_path is None or pytesseract is None:
        raise RuntimeError("OCR dependencies not available. Install pdf2image and pytesseract.")

    pages = convert_from_path(pdf_path, dpi=dpi)
    texts: List[str] = []
    for page in pages:
        text = pytesseract.image_to_string(page)
        texts.append(text)
    return "\n".join(texts)


def find_multiplications_in_text(text: str, tolerance: float = 1.0) -> Dict[str, Any]:
    """Search OCR text for multiplication patterns and verify results.

    Strategy:
    - Find explicit patterns like `200 x 385 = 77000` (various separators)
    - Also scan runs of 3 numbers and check if first*second ~= third

    Returns dict with found_matches and summary pass/fail.
    """
    results: List[Dict[str, Any]] = []

    # pattern: qty x rate = total  (allow x, X, *, × and = or :)
    pat = re.compile(r"(\d+[\.,]?\d*)\s*[xX\*×]\s*(\d+[\.,]?\d*)\s*[=:\-]\s*(\d+[\.,]?\d*)")
    for m in pat.finditer(text):
        a = float(m.group(1).replace(",", ""))
        b = float(m.group(2).replace(",", ""))
        c = float(m.group(3).replace(",", ""))
        prod = a * b
        ok = abs(prod - c) <= tolerance
        results.append({"qty": a, "rate": b, "total": c, "computed": prod, "ok": ok, "match_text": m.group(0)})

    # fallback: look for sequences of three numbers within a short window
    nums = [float(n.replace(",", "")) for n in re.findall(r"(\d+[\.,]?\d*)", text)]
    # scan triples
    for i in range(len(nums) - 2):
        a, b, c = nums[i], nums[i + 1], nums[i + 2]
        prod = a * b
        if abs(prod - c) <= tolerance:
            results.append({"qty": a, "rate": b, "total": c, "computed": prod, "ok": True, "match_text": f"{a} * {b} ~= {c}"})

    summary = {"total_matches": len(results), "all_ok": all(r.get("ok") for r in results) if results else False}
    return {"matches": results, "summary": summary}


def validate_gstin(gstin: str, vendor_name: Optional[str] = None) -> Dict[str, Any]:
    """Validate GSTIN format for Indian GST numbers.

    This function checks:
    - length == 15
    - pattern: 2 digits (state) + 10-char PAN-like + 1 entity char + 'Z' + checksum char
    - state code between 01 and 37 (basic sanity)

    Note: A full checksum validation is not implemented here — this checks format and simple rules.
    """
    gst = (gstin or "").strip().upper()
    result = {"gstin": gst, "valid_format": False, "state_code_ok": False, "notes": []}

    # Optional external GSTIN verification (configurable via env vars).
    # Set `GSTIN_CHECK_URL` to enable an external check (e.g. https://sheet.gstincheck.co.in/check).
    # Optionally set `GSTIN_CHECK_KEY` for an API key; it will be sent as a Bearer token.
    gstin_api_url = os.getenv("gstin_endpoint")
    gstin_api_key = os.getenv("gstin_key")
    if gstin_api_url:
        try:
            headers = {}
            if gstin_api_key:
                headers["Authorization"] = f"Bearer {gstin_api_key}"
            # build safe URL
            url = gstin_api_url.rstrip("/") + "/" + gst
            resp = requests.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                # attach external service response for debugging/decisioning
                try:
                    result["external_check"] = resp.json()
                except Exception:
                    result["external_check"] = {"raw": resp.text}
            else:
                result["notes"].append(f"external_service_error:{resp.status_code}")
        except Exception as e:
            result["notes"].append(f"external_check_error:{str(e)}")

    if len(gst) != 15:
        result["notes"].append("GSTIN must be 15 characters long")
        return result

    # regex for rough PAN-like middle: 5 letters, 4 digits, 1 letter
    import re

    pattern = re.compile(r"^(?P<state>\d{2})(?P<pan>[A-Z]{5}\d{4}[A-Z])(?P<entity>[A-Z0-9])Z(?P<checksum>[A-Z0-9])$")
    m = pattern.match(gst)
    if not m:
        result["notes"].append("GSTIN does not match expected pattern (state+PAN+entity+Z+checksum)")
        return result

    result["valid_format"] = True
    state = int(m.group("state"))
    if 1 <= state <= 37:
        result["state_code_ok"] = True
    else:
        result["notes"].append(f"State code {state} out of expected range 01-37")

    # We could implement checksum validation here; for now we note it's unchecked.
    result["notes"].append("checksum_not_validated")

    # If external check returned a business name, and vendor_name supplied, compare them
    if vendor_name and result.get("external_check"):
        # Try common keys for business name in external response
        ext = result.get("external_check")
        found_name = None
        if isinstance(ext, dict):
            for key in ("business_name", "name", "legal_name", "company", "firm", "trade_name"):
                v = ext.get(key)
                if v:
                    found_name = str(v)
                    break
            # some APIs nest data under 'data' or similar
            if not found_name:
                for parent in ("data", "result", "payload"):
                    if parent in ext and isinstance(ext[parent], dict):
                        for key in ("business_name", "name", "legal_name", "company", "firm", "trade_name"):
                            v = ext[parent].get(key)
                            if v:
                                found_name = str(v)
                                break
                        if found_name:
                            break

        if found_name:
            # compute similarity ratio
            ratio = difflib.SequenceMatcher(a=vendor_name.lower(), b=found_name.lower()).ratio()
            match = ratio >= 0.7
            result["business_name_match"] = {"found_name": found_name, "similarity": round(ratio, 3), "match": match}

    return result
