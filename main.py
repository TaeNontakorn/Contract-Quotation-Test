import os
import re
import json
import tempfile
import time
import asyncio
import logging
from collections import Counter
from typing import Dict, List, Optional
import sys

from dotenv import load_dotenv
from google import genai
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import pdfplumber
import pandas as pd
import magic

from pymongo import MongoClient

from docx import Document



# =========================================================
# CONFIG
# =========================================================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

app = FastAPI()

# แก้ CORS: ถ้าใช้ credentials ต้องระบุ origin จริง ไม่ใช่ *
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://contract-quotation-test-uxkpb378dnukgacrz29msx.streamlit.app/"],  # เปลี่ยนเป็น URL จริงของ Streamlit
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_api_key(form_key: str = "") -> str:
    """ใช้ key จาก form ถ้ามี ไม่งั้น fallback ไป env"""
    if form_key and form_key.strip():
        return form_key.strip()
    return os.getenv("api_key", "")


# Initialize MongoDB ตอน startup ครั้งเดียว
mongo_client = MongoClient(os.getenv("MONGO_URI"))
mongo_db = mongo_client[os.getenv("MONGO_DB_NAME", "law_mango")]
checks_col = mongo_db["document_checks"]

@app.on_event("startup")
async def startup():
    try:
        mongo_client.admin.command("ping")
        logger.info("✅ MongoDB connected")
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")


# =========================================================
# UTIL
# =========================================================

def _sanitize_json_string_values(text: str) -> str:
    """escape literal newline/tab ที่ LLM ใส่ใน string value โดยไม่ escape"""
    result = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            result.append(ch)
            escape_next = False
            continue
        if ch == "\\":
            result.append(ch)
            if in_string:
                escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            continue
        if in_string:
            if ch == "\n":
                result.append("\\n")
            elif ch == "\r":
                result.append("\\r")
            elif ch == "\t":
                result.append("\\t")
            else:
                result.append(ch)
        else:
            result.append(ch)
    return "".join(result)


def safe_json_load(text: str):
    """Fallback parser — ใช้เฉพาะกรณีที่ structured output ไม่ได้เปิดใช้งาน"""
    if not text:
        logger.error("Empty LLM response")
        return {}

    text = text.strip()
    text = re.sub(r"^```json\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"^```\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```$", "", text)

    match = re.search(r"(\{.*\}|\[.*\])", text, re.DOTALL)
    if match:
        text = match.group(0)

    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", text)
    text = _sanitize_json_string_values(text)
    text = re.sub(r',(\s*[}\]])', r'\1', text)

    try:
        return json.loads(text)
    except Exception as e:
        logger.error(f"Invalid JSON from LLM: {e}\n----raw text----\n{text}\n----------------")
        return {}

def sort_by_severity(items: List[dict]) -> List[dict]:
    order = {"สูง": 0, "ปานกลาง": 1, "ต่ำ": 2}
    return sorted(items, key=lambda x: order.get(x.get("severity", "ต่ำ"), 3))


def _parse_date(date_str: str):
    """แปลง DD/MM/YYYY (พ.ศ. หรือ ค.ศ.) เป็น date object — คืน None ถ้าแปลงไม่ได้"""
    if not date_str:
        return None
    from datetime import date as date_cls
    try:
        d, m, y = date_str.strip().split("/")
        d, m, y = int(d), int(m), int(y)
        if y > 2400:   # พ.ศ. → ค.ศ.
            y -= 543
        return date_cls(y, m, d)
    except Exception:
        return None

def build_period_card(cp: dict) -> dict:
    from datetime import date as date_cls

    q_start = _parse_date(cp.get("quotation_start"))
    q_end   = _parse_date(cp.get("quotation_end"))
    c_start = _parse_date(cp.get("contract_start"))
    c_end   = _parse_date(cp.get("contract_end"))

    start = c_start or q_start
    end   = c_end   or q_end

    def fmt(d):
        if not d:
            return "—"
        y = d.year + 543
        return f"{d.day:02d}/{d.month:02d}/{y}"

    q_text = f"วันเริ่มต้น: {fmt(q_start)}  วันสิ้นสุด: {fmt(q_end)}"
    c_text = f"วันเริ่มต้น: {fmt(c_start)}  วันสิ้นสุด: {fmt(c_end)}"

    # เช็คว่าวันที่หายไปฝั่งไหน
    if not q_start and not q_end:
        explanation = "วันที่ในใบเสนอราคาหายไป"
        severity    = "สูง"
    elif not c_start and not c_end:
        explanation = "วันที่ในสัญญาหายไป"
        severity    = "สูง"
    elif start and end:
        total_days = (end - start).days + 1
        try:
            expected_end = start.replace(year=start.year + 1) - __import__("datetime").timedelta(days=1)
            is_full_year = (end == expected_end)
        except ValueError:
            is_full_year = (total_days in (365, 366))

        if is_full_year:
            explanation = f"✅ ครบ 1 ปีพอดี ({total_days:,} วัน)"
            severity    = "ต่ำ"
        else:
            explanation = f"⚠️ ไม่ครบ 1 ปี (ระยะเวลาจริง {total_days:,} วัน)"
            severity    = "สูง"
    else:
        explanation = "❓ ไม่พบข้อมูลวันที่ในเอกสาร"
        severity    = "สูง"

    return {
        "title":          "ระยะเวลาสัญญา",
        "field":          "วันเริ่มต้น-สิ้นสุดสัญญา",
        "type":           "ถ้อยคำต่างแต่ความหมายเหมือนกัน",
        "quotation_text": q_text,
        "contract_text":  c_text,
        "explanation":    explanation,
        "severity":       severity,
        "confidence":     1.0,
    }

# =========================================================
# TEXT HELPERS
# =========================================================

_THAI_MONTHS = {
    "มกราคม": "01", "กุมภาพันธ์": "02", "มีนาคม": "03",
    "เมษายน": "04", "พฤษภาคม": "05", "มิถุนายน": "06",
    "กรกฎาคม": "07", "สิงหาคม": "08", "กันยายน": "09",
    "ตุลาคม": "10", "พฤศจิกายน": "11", "ธันวาคม": "12",
}

def extract_thai_dates(text: str) -> List[str]:
    """แปลงวันที่ภาษาไทย เช่น '1 มกราคม 2567' -> '01/01/2567'"""
    results = []
    for day, month, year in re.findall(r"(\d{1,2})\s*(\S+)\s*(25\d{2})", text):
        if month in _THAI_MONTHS:
            results.append(f"{day.zfill(2)}/{_THAI_MONTHS[month]}/{year}")
    return results


def extract_money(text: str) -> List[dict]:
    """
    ดึงตัวเลขเงินพร้อมข้อมูล VAT จากข้อความ
    คืนค่าเป็น list ของ dict แต่ละตัวมี:
      - amount  : ตัวเลขเงิน (int)
      - include_vat : True/False/None (ถ้าไม่รู้)
      - vat_amount  : ยอด VAT (ถ้าระบุชัด เช่น "VAT 7% = 10,500")
      - raw     : ข้อความต้นฉบับโดยรอบ
    """
    results = []

    # pattern: จับตัวเลขเงินพร้อม context ก่อน-หลัง 60 ตัวอักษร
    money_pattern = re.compile(
        r"([\d,]+(?:\.\d{1,2})?)\s*(?:บาท|baht|THB)?",
        re.IGNORECASE
    )

    # pattern สำหรับ VAT line เช่น "VAT 7% = 10,500" หรือ "ภาษีมูลค่าเพิ่ม 7%"
    vat_line_pattern = re.compile(
        r"(?:vat|ภาษีมูลค่าเพิ่ม|ภาษี\s*7%)[^\d]*([\d,]+(?:\.\d{1,2})?)",
        re.IGNORECASE
    )

    # หา VAT amount ที่ระบุชัดเจนก่อน
    vat_amounts = set()
    for m in vat_line_pattern.finditer(text):
        try:
            vat_amounts.add(int(float(m.group(1).replace(",", ""))))
        except ValueError:
            pass

    # คำที่บ่งบอกว่า รวม VAT แล้ว
    incl_keywords = re.compile(
        r"รวม(?:ภาษี|vat|มูลค่าเพิ่ม)|include\s*vat|incl\.?\s*vat|ราคารวมภาษี",
        re.IGNORECASE
    )
    # คำที่บ่งบอกว่า ยังไม่รวม VAT
    excl_keywords = re.compile(
        r"ไม่(?:รวม|รวมภาษี)|before\s*vat|excl\.?\s*vat|ก่อนภาษี|ราคาก่อนภาษี",
        re.IGNORECASE
    )

    seen = set()
    for m in money_pattern.finditer(text):
        raw_num = m.group(1).replace(",", "")
        try:
            amount = int(float(raw_num))
        except ValueError:
            continue

        # กรองตัวเลขที่น้อยเกินไป (เช่น 7 จาก "VAT 7%") — ราคาจริงต้องมากกว่า 99
        if amount <= 99 or amount in seen:
            continue
        seen.add(amount)

        # context รอบตัวเลข ±80 ตัวอักษร
        start = max(0, m.start() - 80)
        end   = min(len(text), m.end() + 80)
        ctx   = text[start:end]

        include_vat = None
        if incl_keywords.search(ctx):
            include_vat = True
        elif excl_keywords.search(ctx):
            include_vat = False

        # ถ้าตัวเลขนี้ตรงกับ VAT amount ที่พบ → เป็น VAT line ไม่ใช่ราคาหลัก
        is_vat_entry = amount in vat_amounts

        results.append({
            "amount":      amount,
            "include_vat": include_vat,
            "is_vat":      is_vat_entry,
            "raw":         ctx.strip(),
        })

    return results


def detect_user_license(text: str) -> Optional[str]:
    """ตรวจสอบจำนวน User License จากข้อความ"""
    if "unlimited" in text.lower():
        return "Unlimited"
    match = re.search(r"(\d+)\s*(?:user|users)", text.lower())
    if match:
        return match.group(1)
    return None


# =========================================================
# OCR — Gemini
# =========================================================

QUOTATION_OCR_PROMPT = """
    You are a STUPID, BRAINLESS OCR MACHINE. 
    Your ONLY purpose is to copy characters exactly as they appear.
    Don't think Just Focus on Rule

    # STRICTEST RULE:
    - If a name is misspelled (e.g., "Compan" instead of "Company"), EXTRACT IT AS "Compan".
    - If there is a typo in the Thai name, DO NOT FIX IT.
    - If there is a space in the middle of a word, KEEP THE SPACE.
    - Treat every character as a raw shape to be recorded, not as a word to be understood.
    - Do not fix typo

    # OCR INSTRUCTIONS:
    - Extract ALL readable text from this document.
    - Keep original language (Thai / English).
    - Read LEFT to RIGHT, TOP to BOTTOM.
    - Do NOT summarize, translate, or format as JSON.
    - Do NOT "normalize" or "clean up" whitespace unless it's a multi-line table cell.
    
    # CUSTOMER COMPANY NAME (LEGAL DATA):
    - The customer's name is the most sensitive part. 
    - If you change even one character, the legal contract becomes VOID.
    - COPY IT CHARACTER-BY-CHARACTER
    - Don't rewrite customer company name any way
    - CUSTOMER COMPANY name is not "แมงโก้..." or "Mango...."

    TABLE HANDLING:
    - Detect tables explicitly
    - Reconstruct tables as they appear in the document
    - Preserve original row order / column order
    - Do NOT assume a fixed number of columns
    - Use | (pipe) as a column separator

    HEADER HANDLING:
    - Flatten multi-row/grouped headers into a single header row
    - Combine grouped headers using: Parent - Child

    CELL HANDLING:
    - If a cell contains multiple lines, merge into a single cell separated by " / "
    - If a logical row spans multiple visual lines, merge into one row
    - If a cell is empty, leave it empty between pipes

    RULES:
    - Do NOT add, remove, rename, or normalize columns
    - Preserve numbers, units, dates, and symbols exactly as shown
    - If text is unreadable, output [ILLEGIBLE]

    DONTS:
    - Do NOT add any extra commentary or explanations
    - Do NOT extract header/footer of Mango Consultant company

    OUTPUT FORMAT:
    - Insert page separators as: === PAGE N ===
    - Return only the extracted text

    Begin OCR now.
"""

CONTRACT_OCR_PROMPT = """
    You are an OCR engine. Your task is strict text extraction.
    Your ONLY purpose is to copy characters exactly as they appear.
    Extract all visible and readable text exactly as it appears in this document.

    GENERAL RULES:
    - Keep original language (Thai / English)
    - Read content from LEFT to RIGHT, TOP to BOTTOM
    - Return PLAIN TEXT ONLY
    - Do NOT summarize / translate / return JSON or markdown
    - Do NOT fix typo of typo.

    LAYOUT RULES:
    - Join broken lines that are part of the same paragraph or sentence into a single line.
    - Only use new lines (Enter) when a new paragraph or a new clause starts.

    TABLE HANDLING:
    - Detect tables explicitly
    - Reconstruct tables as they appear in the document
    - Preserve original row/column order
    - Use | (pipe) as a column separator

    HEADER HANDLING:
    - Flatten multi-row/grouped headers into a single header row
    - Combine grouped headers using: Parent - Child

    CELL HANDLING:
    - If a cell contains multiple lines, merge into a single cell separated by " / "
    - If a logical row spans multiple visual lines, merge into one row
    - If a cell is empty, leave it empty between pipes

    HALLUCINATION RULES:
    - Extract ONLY text that is visibly present in the document
    - Do NOT infer, guess, or complete missing text
    - If text is unreadable, output [ILLEGIBLE]

    DONTS:
    - Do NOT add any extra commentary or explanations
    - Do NOT extract header/footer of Mango Consultant company

    OUTPUT FORMAT:
    - Insert page separators as: === PAGE N ===
    - Return only the extracted text

    Begin OCR now.
"""


def _ocr_inline_sync(file_bytes: bytes, prompt: str, client: genai.Client) -> str:
    """
    OCR แบบ inline_data — ไม่ต้อง upload ไฟล์ขึ้น Gemini server
    เร็วกว่า file upload โดยเฉพาะไฟล์เล็ก-กลาง (<20MB)
    """
    import base64
    b64 = base64.b64encode(file_bytes).decode()
    response = client.models.generate_content(
        model="gemini-2.5-pro",
        contents=[
            {"inline_data": {"mime_type": "application/pdf", "data": b64}},
            prompt,
        ],
    )
    return response.text.strip()


async def run_ocr(upload: UploadFile, prompt: str, client: genai.Client) -> str:
    """OCR ไฟล์ที่ upload เข้ามา — ใช้ inline_data (ไม่ต้อง upload แยก)"""
    file_bytes = await upload.read()
    return await asyncio.to_thread(_ocr_inline_sync, file_bytes, prompt, client)


async def _ocr_from_bytes(file_bytes: bytes, filename: str, prompt: str, client: genai.Client) -> str:
    """OCR จาก bytes โดยตรง — ใช้สำหรับ Smart File Parser (contract PDF)"""
    return await asyncio.to_thread(_ocr_inline_sync, file_bytes, prompt, client)


# =========================================================
# Word Read
# =========================================================

def _read_paragraph_xml(p_el, qn_func) -> str:
    """
    อ่านข้อความจาก <w:p> element โดยตรงจาก XML
    รองรับ: w:t (text), w:br (line break), w:tab (tab), w:sym (symbol)
    """
    parts = []
    for child in p_el.iter():
        tag = child.tag
        if tag == qn_func("w:t") and child.text:
            parts.append(child.text)
        elif tag == qn_func("w:br"):
            # w:br อาจเป็น line break ภายใน paragraph
            parts.append("\n")
        elif tag == qn_func("w:tab"):
            parts.append("\t")
        elif tag == qn_func("w:sym"):
            # symbol character เช่น checkmark, bullet
            char_val = child.get(qn_func("w:char"))
            if char_val:
                try:
                    parts.append(chr(int(char_val, 16)))
                except (ValueError, OverflowError):
                    pass
    return "".join(parts)


def _extract_textbox_from_element(el, qn_func) -> str:
    """
    ดึงข้อความจาก text box ที่ซ่อนอยู่ใน element (w:drawing หรือ mc:AlternateContent)
    โดยค้นหา w:txbxContent ภายใน element นั้น
    """
    TXBX_TAG = qn_func("w:txbxContent")
    W_P_TAG  = qn_func("w:p")
    lines = []
    for txbx in el.iter(TXBX_TAG):
        for p in txbx.iter(W_P_TAG):
            line = _read_paragraph_xml(p, qn_func)
            if line.strip():
                lines.append(line)
    return "\n".join(lines)


def _extract_textboxes(doc) -> list:
    """
    ดึงข้อความจาก Text Box ทั้งหมดใน document ผ่าน raw XML
    ครอบคลุม: w:txbxContent (text box ปกติ) และ wps:txbx (drawing text box)
    """
    from docx.oxml.ns import qn

    TXBX_TAG = qn("w:txbxContent")
    W_P_TAG  = qn("w:p")

    def read_txbx(txbx_el):
        lines = []
        for p in txbx_el.iter(W_P_TAG):
            lines.append(_read_paragraph_xml(p, qn))
        return "\n".join(lines)

    results = []
    for txbx in doc.element.body.iter(TXBX_TAG):
        text = read_txbx(txbx)
        if text.strip():
            results.append(text.strip())
    return results


def _extract_table_text(table) -> list:
    """
    แปลง table เป็น list ของ string pipe-separated ทีละแถว
    - ลบ duplicate จาก merged cells
    - รักษา newline ภายใน cell
    """
    rows_text = []
    for row in table.rows:
        cells = []
        for cell in row.cells:
            cells.append(cell.text.strip())
        # ลบ duplicate ที่เกิดจาก merged cells
        deduped = []
        for c in cells:
            if not deduped or c != deduped[-1]:
                deduped.append(c)
        rows_text.append(" | ".join(deduped))
    return rows_text


def extract_docx_faithful(file_path: str) -> str:
    """
    ดึงข้อความจาก .docx ให้ตรงกับต้นฉบับมากที่สุด ครอบคลุม:
    - Paragraph ทุกบรรทัด รวมบรรทัดว่าง (การเว้นบรรทัด)
    - Table ทุกแถว (pipe-separated, รองรับ merged cells)
    - Text Box ทุกกล่อง (ดึงผ่าน raw XML)
    - w:t, w:br, w:tab, w:sym — ตัวเลข สัญลักษณ์ และข้อความทุกชนิด
    - อ่าน paragraph และ table ตามลำดับที่ปรากฏจริงในเอกสาร
    """
    from docx.oxml.ns import qn
    from docx.text.paragraph import Paragraph
    from docx.table import Table as DocxTable

    doc = Document(file_path)
    blocks = []

    def iter_block_items(parent):
        """yield Paragraph และ Table ตามลำดับจริงใน body"""
        for child in parent.element.body:
            if child.tag == qn("w:p"):
                yield Paragraph(child, parent)
            elif child.tag == qn("w:tbl"):
                yield DocxTable(child, parent)

    DRAWING_TAG  = qn("w:drawing")
    ALTCONT_TAG = "{http://schemas.openxmlformats.org/markup-compatibility/2006}AlternateContent"

    for block in iter_block_items(doc):
        if isinstance(block, Paragraph):
            # อ่านข้อความปกติจาก paragraph
            text = _read_paragraph_xml(block._element, qn)
            blocks.append(text)

            # ตรวจว่า paragraph นี้มี text box แทรกอยู่ด้วยไหม (inline)
            for child in block._element:
                if child.tag in (DRAWING_TAG, ALTCONT_TAG):
                    tb_text = _extract_textbox_from_element(child, qn)
                    if tb_text.strip():
                        blocks.append(f"[TEXTBOX: {tb_text.strip()}]")

        elif isinstance(block, DocxTable):
            blocks.extend(_extract_table_text(block))

    # ดึง Text Box ที่อยู่นอก paragraph (anchored/floating) ต่อท้าย
    floating_tb = _extract_textboxes(doc)
    # กรองออก text ที่ถูก inline capture ไปแล้ว เพื่อไม่ให้ซ้ำ
    inline_texts = set(b.replace("[TEXTBOX: ", "").rstrip("]") for b in blocks if b.startswith("[TEXTBOX:"))
    floating_new = [t for t in floating_tb if t not in inline_texts]
    if floating_new:
        blocks.append("")
        blocks.append("[TEXT BOX - FLOATING]")
        blocks.extend(floating_new)

    raw = "\n".join(blocks)
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    # จำกัดบรรทัดว่างซ้อนกันไม่เกิน 2 บรรทัด
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


def smart_docx_extract(file_path: str) -> str:
    """
    Entry point สำหรับดึงข้อความจาก .docx
    ดึงครบทุก element: paragraph, table, text box, symbol, tab
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    logger.info("🔍 Extracting docx (faithful full-content extractor)...")
    text = extract_docx_faithful(file_path)
    logger.info(f"✅ Extracted length: {len(text)} characters")
    return text



# =========================================================
# FILTER COMPANY
# =========================================================

def filter_name_company(text: str) -> str:
    customer_line = re.search(r"Customer\s*\|\s*(.+)", text)
    search_text = customer_line.group(1).strip() if customer_line else text

    match = re.search(r"บริษัท\s*(.*?)\s*จำกัด", search_text, re.DOTALL)
    if match:
        return re.sub(r'\s+', ' ', match.group(1).strip())

    # ถ้าหาชื่อบริษัทไม่เจอ คืนค่าว่างแทนที่จะ return text ยาวทั้งก้อน
    return ""


def filter_registration_number(text: str) -> Optional[str]:
    # แก้: เปลี่ยนชื่อตัวแปรไม่ให้ทับ clean_text function
    cleaned = " ".join(text.split())
    my_company_reg = "0105551067687"

    candidates = []
    for match in re.finditer(r"(\d[\d\s\-]{11,18}\d)", cleaned):
        raw_num = match.group(1)
        clean_num = re.sub(r"\D", "", raw_num)
        if len(clean_num) == 13 and clean_num != my_company_reg:
            candidates.append(clean_num)

    return candidates[0] if candidates else None


# =========================================================
# DBD PDF PARSER  (coordinate-based, auto-detect separator)
# =========================================================

class DBDParser:
    """
    ดึงข้อมูลจาก DBD PDF โดยใช้ x,y coordinate ของแต่ละ word
    แทนการพึ่ง line order จาก extract_text()
    รองรับ layout ที่ label/value อยู่ต่าง top กัน
    """

    LABEL_X_MAX = 160
    VALUE_X_MIN = 165
    ROW_TOL = 4

    def __init__(self, file_path: str):
        self.file_path = file_path

    def _detect_separator(self, words):
        colons = [w for w in words if w["text"] == ":"]
        if not colons:
            return
        xs = [round(w["x0"], 0) for w in colons]
        sep_x = Counter(xs).most_common(1)[0][0]
        self.LABEL_X_MAX = sep_x - 1
        self.VALUE_X_MIN = sep_x + 14

    def _get_rows(self):
        with pdfplumber.open(self.file_path) as pdf:
            words = pdf.pages[0].extract_words()
        self._detect_separator(words)
        rows, cur_row, cur_top = [], [], None
        for w in sorted(words, key=lambda x: (x["top"], x["x0"])):
            if cur_top is None or abs(w["top"] - cur_top) <= self.ROW_TOL:
                cur_row.append(w)
                if cur_top is None:
                    cur_top = w["top"]
            else:
                rows.append(cur_row)
                cur_row, cur_top = [w], w["top"]
        if cur_row:
            rows.append(cur_row)
        return rows

    def _split_row(self, row):
        lbl = [w for w in row if w["x0"] < self.LABEL_X_MAX and w["text"] != ":"]
        val = [w for w in row if w["x0"] >= self.VALUE_X_MIN]
        return (
            " ".join(w["text"] for w in lbl).strip(),
            " ".join(w["text"] for w in val).strip(),
            val,
        )

    def _build_field_map(self, rows):
        field_map, label_order, current_lbl = {}, [], None
        for row in rows:
            lbl, val, _ = self._split_row(row)
            if lbl:
                current_lbl = lbl
                if current_lbl not in field_map:
                    field_map[current_lbl] = []
                    label_order.append(current_lbl)
                if val:
                    field_map[current_lbl].append(val)
            elif val and current_lbl:
                field_map[current_lbl].append(val)
        return field_map, label_order

    def _find_field(self, field_map, *keywords):
        for lbl, lines in field_map.items():
            if any(kw in lbl for kw in keywords):
                return " ".join(lines).strip()
        return None

    def _extract_directors(self, rows):
        names, active, seen = [], False, set()
        for row in rows:
            lbl, val, _ = self._split_row(row)
            if "คณะกรรมการลงชื่อผูกพัน" in lbl:
                break
            if ("กรรมการ" in lbl and "คณะ" not in lbl) or (val and re.match(r"\d+\.", val)):
                active = True
            if active and val:
                for m in re.findall(r"\d+\.\s*([^\d/]+)", val):
                    name = m.strip().rstrip("/").strip()
                    if name and name not in seen:
                        names.append(name)
                        seen.add(name)
        return names

    def _extract_binding_directors(self, rows):
        binding_top = None
        for row in rows:
            lbl, _, _ = self._split_row(row)
            if "คณะกรรมการลงชื่อผูกพัน" in lbl:
                binding_top = min(w["top"] for w in row)
                break
        if binding_top is None:
            return None
        stop_kw = ["ข้อควรทราบ", "ข้อจำกัดอำนาจกรรมการ", "URL"]
        parts = []
        for row in rows:
            lbl, val, _ = self._split_row(row)
            row_top = min(w["top"] for w in row)
            if any(kw in lbl or kw in val for kw in stop_kw):
                break
            if val and row_top >= (binding_top - 30) and not re.match(r"\d+\.", val):
                parts.append(val)
        result = re.sub(r"\s+", " ", " ".join(parts)).strip().rstrip("/")
        return result or None

    def _extract_address(self, rows):
        addr_top, next_top, labels_top = None, None, []
        for row in rows:
            lbl, _, _ = self._split_row(row)
            if lbl:
                labels_top.append((min(w["top"] for w in row), lbl))
        for i, (top, lbl) in enumerate(labels_top):
            if "ที่ตั้ง" in lbl:
                addr_top = top
                if i + 1 < len(labels_top):
                    next_top = labels_top[i + 1][0]
                break
        if addr_top is None:
            return None
        parts = []
        for row in rows:
            _, val, _ = self._split_row(row)
            row_top = min(w["top"] for w in row)
            if val and (addr_top - 10) <= row_top < (next_top or float("inf")):
                if re.match(r"^\d{5}\s*:", val):
                    break
                parts.append(val)
        result = re.sub(r"\s+", " ", " ".join(parts)).strip()
        return result or None

    def parse(self) -> dict:
        rows = self._get_rows()
        field_map, _ = self._build_field_map(rows)
        return {
            "registration_number":  self._find_field(field_map, "เลขทะเบียนนิติบุคคล"),
            "status":               self._find_field(field_map, "สถานะนิติบุคคล"),
            "capital":              self._find_field(field_map, "ทุนจดทะเบียน"),
            "address":              self._extract_address(rows),
            "committee":            self._extract_directors(rows),
            "authorized_signatory": self._extract_binding_directors(rows),
        }


# =========================================================
# LLM COMPARE — แยกเป็น 3 functions อิสระ (parallel + focused)
# =========================================================

_GEMINI_CONFIG = {"temperature": 0}  # stable, ไม่สุ่ม


def _gemini_json(client: genai.Client, prompt: str) -> dict:
    """Helper: เรียก Gemini และ parse JSON — ใช้ร่วมกันทั้ง 3 functions"""
    response = client.models.generate_content(
        model="gemini-2.5-pro",
        contents=prompt,
        config=_GEMINI_CONFIG,
    )
    return safe_json_load(response.text)


# ------------------------------------------------------------------
# FUNCTION 1: ตรวจสอบ DBD vs สัญญา
# ------------------------------------------------------------------
def check_dbd_validation(
    contract_text: str,
    dbd_data: dict,
    client: genai.Client,
) -> list:
    """
    ตรวจสอบเฉพาะ DBD กับสัญญา — ไม่รู้จัก Quotation เลย
    ทำให้ model focus 100% กับงานนี้
    """
    prompt = f"""
คุณคือผู้เชี่ยวชาญด้านกฎหมายนิติบุคคลไทย
งานของคุณ: ตรวจสอบว่าข้อมูลในสัญญาสอดคล้องกับข้อมูลจดทะเบียนบริษัท (DBD) หรือไม่

====================================================
ข้อมูล DBD (ข้อมูลอ้างอิงสูงสุด — ถือเป็นความจริง)
====================================================
- เลขทะเบียนนิติบุคคล: {dbd_data.get("registration_number")}
- ที่อยู่ตามทะเบียน: {dbd_data.get("address")}
- รายชื่อกรรมการ: {dbd_data.get("committee")}
- ผู้มีอำนาจลงนาม: {dbd_data.get("authorized_signatory")}

====================================================
กฎการตรวจสอบ
====================================================
1. "ความหมายเหมือนกัน":
   - วิเคราะห์ผลลัพธ์ทางกฎหมาย (Legal Effect) ว่าใครเซ็นได้บ้าง และต้องเซ็นกี่คน
   - DBD ระบุ "พร้อมประทับตราสำคัญ" แต่สัญญาไม่ระบุ → "ข้อมูลขาดในสัญญา"
2. "ขอบเขตอำนาจ":
   - สัญญาระบุชื่อกรรมการครบแต่จำกัดเรื่อง ขณะที่ DBD ไม่ได้จำกัด → "ข้อมูลไม่ตรงกับ DBD"
3. dbd_reference: ต้องคัดลอกข้อความจาก DBD แบบ Word-for-Word เสมอ
4. ผู้มีอำนาจลงนาม:
   - DBD ระบุชื่อบุคคล / สัญญาระบุตำแหน่ง → "ถ้อยคำต่างแต่ความหมายเหมือนกัน" (ถ้าความสามารถลงนามตรงกัน)
   - DBD ระบุ 2 คน / สัญญาระบุ 1 คน → "ข้อมูลขาดในสัญญา"
   - DBD ระบุ 2 คน / สัญญาระบุ "กรรมการบริษัท" ไม่ระบุชื่อ → "ถ้อยคำต่างแต่ความหมายเหมือนกัน"
   - DBD ระบุ 5 คน อย่างน้อย 2 คน / สัญญาระบุ 1 คนหรือไม่ระบุจำนวน → "ข้อมูลขาดในสัญญา"

====================================================
กฎสำคัญสูงสุด
====================================================
- ห้ามคาดเดาข้อมูลที่ไม่มี
- ตอบเป็น JSON array เท่านั้น ไม่มี key ห่อนอก
- ถ้าไม่พบความผิดปกติใดเลย ให้ตอบ []

====================================================
ข้อมูลนำเข้า — สัญญา
====================================================
<<<
{contract_text}
>>>

====================================================
รูปแบบผลลัพธ์ (JSON array เท่านั้น)
====================================================
[
  {{
    "title": "ประโยคสรุปสั้นๆ เช่น 'ที่อยู่ในสัญญาไม่ตรงกับ DBD'",
    "field": "เลขทะเบียนนิติบุคคล | ที่อยู่ตามทะเบียน | รายชื่อกรรมการ | ผู้มีอำนาจลงนาม",
    "type": "ข้อมูลไม่ตรงกับ DBD | ข้อมูลขาดในสัญญา | ถ้อยคำต่างแต่ความหมายเหมือนกัน",
    "contract_text": "...",
    "dbd_reference": "...",
    "explanation": "อธิบายเป็นภาษาไทย โดยอ้างอิง DBD",
    "severity": "ต่ำ | ปานกลาง | สูง",
    "confidence": 0.0
  }}
]
"""
    result = _gemini_json(client, prompt)
    return result if isinstance(result, list) else []


# ------------------------------------------------------------------
# FUNCTION 2: เปรียบเทียบ Quotation vs Contract + ดึงวันที่
# ------------------------------------------------------------------
def check_document_comparison(
    quotation_text: str,
    contract_text: str,
    client: genai.Client,
) -> dict:
    """
    เปรียบเทียบ Quotation กับ Contract — ไม่รู้จัก DBD เลย
    คืนค่า: { "contract_period": {...}, "document_comparison": [...] }
    """
    prompt = f"""
คุณคือผู้เชี่ยวชาญด้านเอกสารพาณิชย์และสัญญาซอฟต์แวร์
งานของคุณ: เปรียบเทียบใบเสนอราคา (Quotation) กับสัญญา (Contract) เพื่อหาความไม่สอดคล้อง

====================================================
กฎหลัก
====================================================
- ใบเสนอราคา คือข้อมูลเชิงพาณิชย์ที่ถูกต้องที่สุด — ใช้เป็น reference
- ห้ามใช้ DBD หรืออ้างอิงข้อมูลนอกเอกสารทั้งสอง
- ห้ามระบุเลขหน้าของใบเสนอราคาในผลลัพธ์
- ไม่ต้องตรวจจับ หัก % ณ ที่จ่าย

====================================================
ฟิลด์ที่ต้องตรวจสอบ
====================================================

[กำหนดระยะเวลา]
- ดึงวันที่เริ่มต้นและสิ้นสุดจากทั้งสองเอกสาร → ใส่ใน contract_period
- ห้ามคำนวณจำนวนวัน — ดึงแค่วันที่ดิบ
- รูปแบบ: DD/MM/YYYY หรือ null ถ้าไม่พบ

[การชำระเงินและขอบเขต]
- "Mango Anywhere Software - ยอดรวม": ตรวจยอด Total Amount ทุกจุด
- "ค่าสิทธิ (Royalty Fee)": ตรวจเงื่อนไขการชำระแยกตามรายการ
- "เงินมัดจำ/เงินประกัน": ตรวจยอดให้ตรงตามข้อตกลง

[VAT]
- ตรวจว่า "รวม VAT แล้ว" หรือ "ยังไม่รวม VAT" ตรงกันทุกจุดที่มีราคา
- ตรวจยอด VAT amount ว่าตรงกับ % ที่ระบุหรือไม่

[สิทธิการใช้งาน / จำนวนผู้ใช้]
- field = "User License / จำนวนผู้ใช้งาน"
- ค้นหาทุกคำ: "user", "users", "ผู้ใช้งาน", "license", "seat", "account", "named user", "concurrent user", "จำนวนผู้ใช้", "สิทธิ์การใช้งาน"
- ถ้า Quotation ระบุจำนวนแต่ Contract ไม่ระบุหรือไม่ตรงกัน → severity = "สูง"

[การบอกเลิกสัญญา]
- field = "การบอกเลิกสัญญา"
- ตรวจ: Notice Period / เหตุผลบอกเลิก / ผลกระทบ (คืนเงิน, ค่าปรับ)
- Quotation มีแต่ Contract ไม่มีหรือต่างกัน → "ข้อมูลขาดในสัญญา" หรือ "แตกต่างอย่างมีนัยสำคัญ"

[ข้อยกเว้น — ไม่ต้องรายงาน]
- ITAP (สวทช. สนับสนุน 50%, ไม่เกิน 150,000 บาท) หายจากสัญญา
- "สัญญาฉบับนี้ทำขึ้นที่ ... เมื่อวันที่..."
- ค่าเดินทาง/ที่พัก (>80 กม. หรือนอกกรุงเทพฯ ปริมณฑล)

ประเภทที่ต้องใช้:
"อาจเป็นการสะกดผิด" | "ถ้อยคำต่างแต่ความหมายเหมือนกัน" | "แตกต่างเล็กน้อย" |
"แตกต่างอย่างมีนัยสำคัญ" | "ข้อมูลขาดในสัญญา" | "ข้อมูลขาดทั้งสองฝ่าย"

====================================================
กฎสำคัญสูงสุด
====================================================
- ห้ามคาดเดาข้อมูลที่ไม่มี
- ตอบเป็น JSON object เท่านั้น

====================================================
ข้อมูลนำเข้า
====================================================
ใบเสนอราคา:
<<<
{quotation_text}
>>>

สัญญา:
<<<
{contract_text}
>>>

====================================================
รูปแบบผลลัพธ์ (JSON object เท่านั้น)
====================================================
{{
  "contract_period": {{
    "quotation_start": "DD/MM/YYYY หรือ null",
    "quotation_end":   "DD/MM/YYYY หรือ null",
    "contract_start":  "DD/MM/YYYY หรือ null",
    "contract_end":    "DD/MM/YYYY หรือ null"
  }},
  "document_comparison": [
    {{
      "title": "ประโยคสรุปสั้นๆ เช่น 'จำนวน User License ในสัญญาไม่ตรงกับใบเสนอราคา'",
      "field": "ชื่อบริษัท | ราคา | จำนวนผู้ใช้งาน | วันที่ | หน้าที่ความรับผิดชอบ | อื่น ๆ",
      "type": "อาจเป็นการสะกดผิด | ถ้อยคำต่างแต่ความหมายเหมือนกัน | แตกต่างเล็กน้อย | แตกต่างอย่างมีนัยสำคัญ | ข้อมูลขาดในสัญญา | ข้อมูลขาดทั้งสองฝ่าย",
      "quotation_text": "ข้อความจากใบเสนอราคาที่เกี่ยวข้อง",
      "contract_text": "ข้อที่ N ...",
      "explanation": "อธิบายเป็นภาษาไทย",
      "severity": "ต่ำ | ปานกลาง | สูง",
      "confidence": 0.0
    }}
  ]
}}
"""
    result = _gemini_json(client, prompt)
    return result if isinstance(result, dict) else {}


# ------------------------------------------------------------------
# FUNCTION 3: ตรวจความสอดคล้องภายในสัญญา
# ------------------------------------------------------------------
def check_internal_consistency(
    contract_text: str,
    client: genai.Client,
) -> list:
    """
    ตรวจสอบเฉพาะภายในสัญญา — ไม่รู้จัก Quotation และ DBD เลย
    คืนค่า: list ของ issues
    """
    prompt = f"""
คุณคือผู้เชี่ยวชาญด้านการตรวจทานสัญญา (Contract Proofreader)
งานของคุณ: ตรวจสอบความสอดคล้องและความถูกต้องภายในสัญญาฉบับเดียว

====================================================
สิ่งที่ต้องตรวจสอบ
====================================================
1. ความสอดคล้องของข้อมูลที่ควรเหมือนกันตลอดทั้งสัญญา
2. การสะกดผิด — ถ้าพบคำผิดซ้ำหลายจุด ให้รายงานแยกทุกจุด
3. เลขข้อ — ตรวจว่าเรียงลำดับถูกต้อง

กฎการตรวจวันที่:
- วันที่เริ่มสัญญาต้องตรงกันทั้งหมดในทุกข้อที่อ้างอิง (เช่น "Effective Date", "วันที่มีผลบังคับใช้")
- วันที่สิ้นสุดสัญญาต้องตรงกันทั้งหมดในทุกข้อที่อ้างอิง (เช่น "Expiration Date", "วันที่หมดอายุ")
- ข้อยกเว้น: "สัญญาฉบับนี้ทำขึ้นที่ บริษัท แมงโก้ คอนซัลแตนท์ จำกัด เมื่อวันที่..." ไม่ต้องแจ้งเตือน

กฎการตรวจคำผิด:
- ถ้าพบคำสะกดผิด ให้ครอบด้วย <strong style="color: red;">คำผิด</strong>
- ใช้ type = "อาจเป็นการสะกดผิด", field = "คำที่อาจสะกดผิด"
- อธิบายคำที่ถูกต้องและความหมายในบริบทสัญญา

กฎการตรวจพยาน:
- ถ้าต้องเซ็นเป็นพยานแต่ไม่ระบุชื่อ ไม่ต้องแสดง

====================================================
กฎสำคัญสูงสุด
====================================================
- ตรวจสอบเฉพาะภายในสัญญา — ห้ามอ้างอิงเอกสารอื่น
- ห้ามคาดเดาข้อมูลที่ไม่มี
- ตอบเป็น JSON array เท่านั้น
- ถ้าไม่พบความผิดปกติใดเลย ให้ตอบ []

====================================================
ข้อมูลนำเข้า — สัญญา
====================================================
<<<
{contract_text}
>>>

====================================================
รูปแบบผลลัพธ์ (JSON array เท่านั้น)
====================================================
[
  {{
    "title": "ประโยคสรุปสั้นๆ เช่น 'วันที่ในข้อ 1.1 ไม่ตรงกับวันที่ในข้อ 5.2'",
    "field": "เนื้อหาในสัญญา | วันที่ | เลขข้อ | คำที่อาจสะกดผิด | อื่นๆ",
    "type": "แตกต่างอย่างมีนัยสำคัญ | อาจเป็นการสะกดผิด | เลขข้อไม่เรียงกัน | วันที่ไม่ตรงกัน | ข้อมูลขาดในสัญญา",
    "contract_text": "ข้อที่ N ...",
    "explanation": "อธิบายเป็นภาษาไทย",
    "severity": "ต่ำ | ปานกลาง | สูง",
    "confidence": 0.0
  }}
]
"""
    result = _gemini_json(client, prompt)
    return result if isinstance(result, list) else []


# ------------------------------------------------------------------
# ORCHESTRATOR: รัน 3 functions แบบ parallel แล้วรวมผล
# ------------------------------------------------------------------
async def compare_documents_parallel(
    quotation_text: str,
    contract_text: str,
    dbd_data: dict,
    client: genai.Client,
) -> dict:
    """
    รัน check_dbd_validation, check_document_comparison, check_internal_consistency
    พร้อมกัน (parallel) ด้วย asyncio.gather
    เร็วขึ้น ~2-3x เทียบกับ sequential และแม่นขึ้นเพราะแต่ละ call มี focused prompt
    """
    logger.info("[ LLM ] เริ่ม 3 parallel checks...")

    dbd_task      = asyncio.to_thread(check_dbd_validation,       contract_text, dbd_data, client)
    doc_task      = asyncio.to_thread(check_document_comparison,   quotation_text, contract_text, client)
    internal_task = asyncio.to_thread(check_internal_consistency,  contract_text, client)

    dbd_issues, doc_result, internal_issues = await asyncio.gather(
        dbd_task, doc_task, internal_task,
        return_exceptions=True,
    )

    # handle exceptions ในแต่ละ branch อย่างอิสระ
    if isinstance(dbd_issues, Exception):
        logger.error(f"[ DBD CHECK ERROR ] {dbd_issues}")
        dbd_issues = []
    if isinstance(doc_result, Exception):
        logger.error(f"[ DOC CHECK ERROR ] {doc_result}")
        doc_result = {}
    if isinstance(internal_issues, Exception):
        logger.error(f"[ INTERNAL CHECK ERROR ] {internal_issues}")
        internal_issues = []

    logger.info("[ LLM ] ทั้ง 3 checks เสร็จสิ้น")

    return {
        "dbd_validation":                dbd_issues,
        "contract_period":               doc_result.get("contract_period", {}),
        "document_comparison":           doc_result.get("document_comparison", []),
        "contract_internal_consistency": internal_issues,
    }


# =========================================================
#                        MongoDB
# =========================================================
# mongo_client, mongo_db, checks_col ถูก initialize ด้านบนแล้ว


# =========================================================
# FASTAPI ENDPOINTS
# =========================================================

@app.get("/")
async def root():
    return {"message": "API is running"}


@app.get("/history")
async def get_history():
    """ดึงประวัติการตรวจสอบทั้งหมดจาก MongoDB"""
    try:
        records = []
        for doc in checks_col.find({}, {"_id": 0}):
            records.append(doc)
        return records
    except Exception as e:
        logger.error(f"[ HISTORY ERROR ] {e}")
        return []


@app.post("/recheck")
async def recheck(
    quotation: List[UploadFile] = File(...),
    contract:     UploadFile = File(...),
    dbd_pdf:      UploadFile = File(...),
    checker_name: str = Form("—"),
    api_key: str = Form(""),
):
    key = get_api_key(api_key)
    client = genai.Client(api_key=key)

    try:
        # ===============================
        # STEP 1: อ่านไฟล์ทั้งหมดพร้อมกัน
        # ===============================
        logger.info("[ READ ] อ่าน bytes ทั้งหมด...")

        # อ่าน bytes พร้อมกัน
        quotation_bytes_list, contract_bytes, dbd_bytes = await asyncio.gather(
            asyncio.gather(*[f.read() for f in quotation]),
            contract.read(),
            dbd_pdf.read(),
        )
        contract_ext = (contract.filename or "").rsplit(".", 1)[-1].lower()

        # ===============================
        # STEP 2: OCR + DBD parse แบบ parallel
        # ===============================
        logger.info("[ PARALLEL ] เริ่ม OCR + DBD parse พร้อมกัน...")

        # สร้าง OCR tasks สำหรับ quotation ทุกไฟล์
        async def ocr_quotation_file(idx: int, raw_bytes: bytes, filename: str) -> str:
            text = await asyncio.to_thread(_ocr_inline_sync, raw_bytes, QUOTATION_OCR_PROMPT, client)
            return f"\n--- QUOTATION {filename} ---\n{text}"

        quotation_tasks = [
            ocr_quotation_file(i, raw_bytes, quotation[i].filename)
            for i, raw_bytes in enumerate(quotation_bytes_list)
        ]

        # Contract: PDF → OCR inline, DOCX → python-docx (sync)
        async def extract_contract() -> str:
            if contract_ext == "pdf":
                logger.info("[ SMART PARSER ] Contract เป็น PDF → ใช้ OCR inline")
                return await asyncio.to_thread(_ocr_inline_sync, contract_bytes, CONTRACT_OCR_PROMPT, client)
            else:
                logger.info("[ SMART PARSER ] Contract เป็น DOCX → ใช้ python-docx")
                suffix = f".{contract_ext}"
                tmp_path = None
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                        tmp.write(contract_bytes)
                        tmp_path = tmp.name
                    return await asyncio.to_thread(smart_docx_extract, tmp_path)
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        os.remove(tmp_path)

        # DBD parse (sync ใน thread)
        async def parse_dbd_async() -> dict:
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                    tmp.write(dbd_bytes)
                    tmp_path = tmp.name
                return await asyncio.to_thread(lambda: DBDParser(tmp_path).parse())
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)

        # รันทั้งหมดพร้อมกัน: OCR quotation(s) + OCR contract + DBD parse
        all_results = await asyncio.gather(
            *quotation_tasks,
            extract_contract(),
            parse_dbd_async(),
            return_exceptions=True,
        )

        # แยกผลลัพธ์
        n_q = len(quotation_tasks)
        quotation_parts = all_results[:n_q]
        contract_result = all_results[n_q]
        dbd_result      = all_results[n_q + 1]

        # รวม quotation text
        quotation_text = ""
        for part in quotation_parts:
            if isinstance(part, Exception):
                logger.error(f"[ OCR QUOTATION ERROR ] {part}")
            else:
                quotation_text += part

        # contract text
        if isinstance(contract_result, Exception):
            logger.error(f"[ OCR CONTRACT ERROR ] {contract_result}")
            contract_text = ""
        else:
            contract_text = contract_result

        # dbd data
        dbd_data: dict = {}
        if isinstance(dbd_result, Exception):
            logger.error(f"[ DBD PARSE ERROR ] {dbd_result}")
        else:
            dbd_data = dbd_result

        logger.info("[ PARALLEL ] OCR + DBD เสร็จสิ้น")
        logger.info(f"[ TEXT CHECK ] Quotation ({len(quotation_text)} chars) preview: {quotation_text[:300]}")
        logger.info(f"[ TEXT CHECK ] Contract ({len(contract_text)} chars) preview: {contract_text[:300]}")

        # ===============================
        # STEP 3: FILTER DATA
        # ===============================
        company_name        = filter_name_company(quotation_text)
        registration_number = filter_registration_number(contract_text)

        # DBD reg matching
        reg_match = False
        dbd_reg = dbd_data.get("registration_number")
        if registration_number and dbd_reg and registration_number == dbd_reg:
            reg_match = True
        if not registration_number and dbd_reg:
            registration_number = dbd_reg
            logger.warning(f"[ REG FALLBACK ] ใช้เลขทะเบียนจาก DBD: {dbd_reg}")

        # ===============================
        # STEP 4: LLM — 3 checks แบบ parallel
        # ===============================
        result_raw = await compare_documents_parallel(
            quotation_text, contract_text, dbd_data, client
        )

        # inject period card
        cp          = result_raw.get("contract_period") or {}
        period_card = build_period_card(cp)
        doc_cmp     = (
            ([period_card] if period_card is not None else [])
            + result_raw.get("document_comparison", [])
        )

        # คำนวณ overall_risk จาก severity ของทุก issue
        all_issues = (
            result_raw.get("dbd_validation", [])
            + doc_cmp
            + result_raw.get("contract_internal_consistency", [])
        )
        severity_counts = {"สูง": 0, "ปานกลาง": 0, "ต่ำ": 0}
        for issue in all_issues:
            sev = issue.get("severity", "ต่ำ")
            if sev in severity_counts:
                severity_counts[sev] += 1
        if severity_counts["สูง"] > 0:
            overall_risk = "สูง"
        elif severity_counts["ปานกลาง"] > 0:
            overall_risk = "ปานกลาง"
        else:
            overall_risk = "ต่ำ"

        result = {
            "summary":                       {"overall_risk": overall_risk},
            "dbd_validation":                sort_by_severity(result_raw.get("dbd_validation", [])),
            "document_comparison":           sort_by_severity(doc_cmp),
            "contract_internal_consistency": sort_by_severity(result_raw.get("contract_internal_consistency", [])),
            "raw_dbd_data":                  dbd_data,
            "dbd_reg_match":                 reg_match,
        }

        # ===============================
        # STEP 5: SAVE TO MONGODB
        # ===============================
        safe_reg = registration_number or "unknown"
        try:
            now_ts = int(time.time())
            quotation_filenames = sorted([f.filename for f in quotation])
            existing = checks_col.find_one({
                "registration_number": safe_reg,
                "quotation_filename":  quotation_filenames,
                "contract_filename":   contract.filename,
                "checker_name":        checker_name,
                "created_at":          {"$gte": now_ts - 1},
            })
            if existing:
                logger.warning("[ MONGODB ] Duplicate detected — skipping insert")
            else:
                checks_col.insert_one({
                    "registration_number": safe_reg,
                    "company_name":        company_name,
                    "checker_name":        checker_name,
                    "quotation_filename":  quotation_filenames,
                    "contract_filename":   contract.filename,
                    "dbd_filename":        dbd_pdf.filename,
                    "result":              result,
                    "created_at":          now_ts,
                })
                logger.info("[ MONGODB ] Saved successfully")
        except Exception as e:
            logger.error(f"[ MONGODB ERROR ] {e}")

        return result

    except Exception as e:
        import traceback
        logger.error(f"SYSTEM ERROR: {e}\n{traceback.format_exc()}")
        return {"error": str(e)}


