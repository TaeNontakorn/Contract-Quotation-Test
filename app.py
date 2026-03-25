import streamlit as st
import requests
import base64
from datetime import datetime
import mammoth

st.set_page_config(
    page_title="C&Q Recheck",
    layout="wide"
)

API_URL = "https://contract-quotation-test-1.onrender.com/recheck"
HISTORY_URL = "https://contract-quotation-test-1.onrender.com/history"
# =========================
# Custom CSS
# =========================
st.markdown("""
<style>
.summary-container {
    display: flex;
    gap: 10px;
    margin-bottom: 25px;
}
.summary-item {
    padding: 6px 16px;
    border-radius: 6px;
    font-weight: bold;
    font-size: 18px;
    display: flex;
    align-items: center;
}
.sum-high { background-color: #ff4d4d; color: white; }
.sum-medium { background-color: #ffa500; color: white; }
.sum-low { background-color: #ffd700; color: #333; }

.box-high {
    background-color: #fff5f5;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid #ffcccc;
    border-left: 10px solid #ff4d4d;
    margin-bottom: 20px;
}
.box-medium {
    background-color: #fffaf0;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid #ffe4b3;
    border-left: 10px solid #ffa500;
    margin-bottom: 20px;
}
.box-low {
    background-color: #fffff0;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid #fff9c4;
    border-left: 10px solid #ffd700;
    margin-bottom: 20px;
}

.badge {
    font-size: 16px;
    font-weight: bold;
    padding: 4px 14px;
    border-radius: 6px;
    display: inline-flex;
    align-items: center;
    margin-bottom: 20px;
}
.badge::before {
    content: "●";
    margin-right: 10px;
    font-size: 20px;
}
.badge-high { background-color: #ff4d4d; color: white; }
.badge-medium { background-color: #ffa500; color: white; }
.badge-low { background-color: #ffd700; color: #333; }

.card-title {
    font-size: 17px;
    font-weight: bold;
    color: #1a1a1a;
    margin-bottom: 14px;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(0,0,0,0.08);
}

.content-row {
    margin-bottom: 10px;
    font-size: 16px;
    color: #000;
}
.label {
    font-weight: bold;
    color: #000;
}
.text-red {
    color: #000;
}

.dbd-warning {
    background-color: #fff3cd;
    border: 1px solid #ffc107;
    border-left: 6px solid #ffc107;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 20px;
    font-size: 15px;
    color: #856404;
}

/* History card */
.history-card {
    background-color: #f9f9f9;
    border: 1px solid #e0e0e0;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 14px;
    cursor: pointer;
}
.history-card:hover {
    background-color: #f0f4ff;
    border-color: #6c8ebf;
}
.history-meta {
    font-size: 13px;
    color: #888;
    margin-top: 6px;
}
.risk-high   { color: #ff4d4d; font-weight: bold; }
.risk-medium { color: #ffa500; font-weight: bold; }
.risk-low    { color: #6aab1e; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# =========================================================
# Severity Helpers
# =========================================================
def map_severity(severity: str) -> str:
    severity = str(severity).strip()
    mapping = {
        "สูง": "high", "ปานกลาง": "medium", "ต่ำ": "low",
        "high": "high", "medium": "medium", "low": "low"
    }
    return mapping.get(severity, "low")

def sort_by_severity(items):
    priority = {"high": 0, "medium": 1, "low": 2}
    return sorted(items, key=lambda x: priority.get(map_severity(x.get("severity", "")), 3))

def count_severity(items):
    counts = {"high": 0, "medium": 0, "low": 0}
    for item in items:
        sev = map_severity(item.get("severity", ""))
        if sev in counts:
            counts[sev] += 1
    return counts

def render_pdf(file):
    b64 = base64.b64encode(file.getvalue()).decode("utf-8")
    st.markdown(f"""
        <iframe src="data:application/pdf;base64,{b64}"
            width="100%" height="600px" type="application/pdf">
        </iframe>
    """, unsafe_allow_html=True)

def _safe(val):
    """คง <strong> ไว้เพื่อ highlight คำผิดสีแดง แต่ลบ tag อื่นทั้งหมดออก"""
    import re as _re, html as _html
    text = str(val) if val is not None else '—'
    parts = _re.split(r'(<strong[^>]*>.*?</strong>)', text, flags=_re.DOTALL)
    result = []
    for part in parts:
        if _re.match(r'<strong[^>]*>', part, flags=_re.DOTALL):
            result.append(part)  # คง <strong> ไว้
        else:
            result.append(_html.unescape(_re.sub(r'<[^>]+>', '', part)))
    return ''.join(result)

def render_cards(items, fields):
    for item in sort_by_severity(items):
        sev_raw   = item.get("severity", "ต่ำ")
        sev_class = map_severity(sev_raw)
        title     = _safe(item.get("title", ""))
        title_html = f"<div class='card-title'>{title}</div>" if title else ""
        rows_html = "".join(
            f"<div class='content-row'><span class='label'>{label} :</span> "
            f"<span class='{css}'>{_safe(item.get(key, '—'))}</span></div>"
            for label, key, css in fields
        )
        st.markdown(f"""
        <div class='box-{sev_class}'>
            <div class='badge badge-{sev_class}'>{sev_raw}</div>
            {title_html}
            {rows_html}
        </div>
        """, unsafe_allow_html=True)

def render_result(result):
    """Render ผลการตรวจสอบ (ใช้ร่วมกันระหว่าง tab ตรวจสอบ และ tab ประวัติ)"""
    all_dbd      = result.get("dbd_validation", [])
    all_doc      = result.get("document_comparison", [])
    all_internal = result.get("contract_internal_consistency", [])
    counts       = count_severity(all_dbd + all_doc + all_internal)

    if not result.get("dbd_reg_match", True):
        raw_dbd = result.get("raw_dbd_data", {})
        st.markdown(f"""
        <div class='dbd-warning'>
            ⚠️ <strong>เลขทะเบียนนิติบุคคลในสัญญาไม่ตรงกับ DBD PDF ที่อัปโหลด</strong><br>
            DBD PDF นี้เป็นของบริษัท เลขทะเบียน: <strong>{raw_dbd.get('registration_number', '—')}</strong>
            — ผลการตรวจสอบ DBD Validation อาจไม่ถูกต้อง
        </div>
        """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class='summary-container'>
        <div class='summary-item sum-high'>🔴 {counts['high']} สูง</div>
        <div class='summary-item sum-medium'>🟠 {counts['medium']} ปานกลาง</div>
        <div class='summary-item sum-low'>🟡 {counts['low']} ต่ำ</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("## 🏢 DBD เปรียบเทียบกับ Contract ")
    if not all_dbd:
        st.success("ไม่พบความผิดปกติด้าน DBD")
    else:
        render_cards(all_dbd, [
           
            ("ข้อความในสัญญา",  "contract_text", ""),
            ("อ้างอิง DBD",      "dbd_reference", ""),
            ("คำอธิบาย",         "explanation",   ""),
        ])

    st.markdown("---")
    st.markdown("## 📄 Quotation เปรียบเทียบกับ Contract ")
    if not all_doc:
        st.success("ไม่พบความแตกต่างระหว่างเอกสาร")
    else:
        render_cards(all_doc, [
            
            ("Quotation", "quotation_text", ""),
            ("Contract",  "contract_text",  "text-red"),
            ("คำอธิบาย",  "explanation",    ""),
        ])

    st.markdown("---")
    st.markdown("## 📑 ความถูกต้องของสัญญา ")
    if not all_internal:
        st.success("ไม่พบความไม่สอดคล้องภายในสัญญา")
    else:
        render_cards(all_internal, [
            ("ตำแหน่งในสัญญา", "contract_text", "text-red"),
            ("คำอธิบาย",        "explanation",   ""),
        ])

# =========================
# CHECKER LIST
# =========================
CHECKER_NAMES = [
    "— เลือกชื่อผู้ตรวจสอบ —",
    "พี่อัง",
    "พี่หนิง",
    "ตะวัน",
    "แพรว",
    "เต้",
    "อื่น ๆ (กรอกเอง)",
]

# =========================
# UI Header & Tabs
# =========================
st.title("📄 C&Q Recheck")
st.subheader("ระบบตรวจสอบความสอดคล้องระหว่างใบเสนอราคาและสัญญา")

tab_check, tab_history = st.tabs(["🔍 ตรวจสอบเอกสาร", "🕓 ประวัติการตรวจสอบ"])

# =========================================================
# TAB 1 — ตรวจสอบเอกสาร
# =========================================================
with tab_check:

    # ── Checker dropdown ──
    st.markdown("### 👤 ผู้ตรวจสอบ")
    selected_checker = st.selectbox("เลือกชื่อผู้ตรวจสอบ", CHECKER_NAMES, key="checker_select")
    
    api_key = st.text_input("API Key", key="API_key_input")
    
    custom_name = ""
    if selected_checker == "อื่น ๆ (กรอกเอง)":
        custom_name = st.text_input("กรอกชื่อผู้ตรวจสอบ", key="custom_checker")

    checker_name = custom_name.strip() if selected_checker == "อื่น ๆ (กรอกเอง)" else selected_checker

    st.markdown("---")

    # ── แถวที่ 1: ใบเสนอราคา ──
    row1_left, row1_right = st.columns([1, 2])

    with row1_left:
        st.markdown("### 📑 ใบเสนอราคา (Quotation)")

        quotation_files = st.file_uploader(
            "อัปโหลดไฟล์ PDF",
            type=["pdf"],
            key="quotation",
            accept_multiple_files=True
        )

        if quotation_files:
            st.markdown("✔️ **อัปโหลดสำเร็จ**")
        else:
            st.markdown("❌ **ยังไม่ได้อัปโหลด**")

    with row1_right:
        if quotation_files:
            for file in quotation_files:
                render_pdf(file)

    st.markdown("---")

    # ── แถวที่ 2: สัญญา ──
    row2_left, row2_right = st.columns([1, 2])
    with row2_left:
        st.markdown("### 📜 สัญญา (Contract)")
        contract_file = st.file_uploader(
            "อัปโหลดไฟล์ PDF หรือ Word (.pdf, .docx)",
            type=["pdf", "docx"],
            key="contract"
        )
        if contract_file:
            ext = contract_file.name.rsplit(".", 1)[-1].lower()
            icon = "📄" if ext == "docx" else "📑"
            st.markdown(f"✔️ **อัปโหลดสำเร็จ** — {icon} ตรวจพบไฟล์ **{ext.upper()}**")
        else:
            st.markdown("❌ **ยังไม่ได้อัปโหลด**")
    with row2_right:
        if contract_file:
            ext = contract_file.name.rsplit(".", 1)[-1].lower()
            if ext == "pdf":
                render_pdf(contract_file)
            else:
                st.info(f"📄 {contract_file.name} (Word document)")

    st.markdown("---")

    # ── แถวที่ 3: DBD ──
    row3_left, row3_right = st.columns([1, 2])
    with row3_left:
        st.markdown("### 🏢 DBD (ข้อมูลบริษัทจาก DBD)")
        dbd_file = st.file_uploader("อัปโหลดไฟล์ PDF", type=["pdf"], key="dbd")
        if dbd_file:
            st.markdown("✔️ **อัปโหลดสำเร็จ**")
        else:
            st.markdown("❌ **ยังไม่ได้อัปโหลด**")
    with row3_right:
        if dbd_file:
            render_pdf(dbd_file)

    st.markdown("---")

    if st.button("🔍 ตรวจสอบเอกสาร", use_container_width=True):

        # ── validation ──
        missing = []
        if checker_name == "— เลือกชื่อผู้ตรวจสอบ —" or not checker_name:
            missing.append("ชื่อผู้ตรวจสอบ")
        if not quotation_files:
            missing.append("ใบเสนอราคา")
        if not contract_file:
            missing.append("สัญญา")
        if not dbd_file:
            missing.append("DBD PDF")
        if missing:
            st.warning(f"กรุณากรอก / อัปโหลด: {', '.join(missing)}")
            st.stop()

        # ── call API ──
        with st.spinner("กำลังประมวลผลเอกสาร..."):
            
            # เลือก MIME type ตามนามสกุลไฟล์ที่อัปโหลด
            contract_ext  = contract_file.name.rsplit(".", 1)[-1].lower()
            contract_mime = (
                "application/pdf"
                if contract_ext == "pdf"
                else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
            files = [
                ("contract", (contract_file.name, contract_file.getvalue(), contract_mime)),
                ("dbd_pdf",  (dbd_file.name,      dbd_file.getvalue(),      "application/pdf")),
            ]
            for qf in quotation_files:
                files.append(("quotation", (qf.name, qf.getvalue(), "application/pdf")))

            data = {"checker_name": checker_name,
                    "api_key": api_key}

            try:
                response = requests.post(API_URL, files=files, data=data, timeout=1000)
                response.raise_for_status()
                result = response.json()
            except requests.exceptions.ConnectionError:
                st.error("ไม่สามารถเชื่อมต่อ API ได้ — กรุณาตรวจสอบว่า backend รันอยู่ที่ port 8000")
                st.stop()
            except Exception as e:
                st.error(f"เกิดข้อผิดพลาด: {e}")
                st.stop()

        st.success(f"✅ ตรวจสอบเสร็จสิ้น — บันทึกโดย **{checker_name}**")
        render_result(result)

# =========================================================
# TAB 2 — ประวัติการตรวจสอบ
# =========================================================
with tab_history:
    st.markdown("### 🕓 ประวัติการตรวจสอบ ")

    col_refresh, col_filter = st.columns([1, 3])
    with col_refresh:
        refresh = st.button("🔄 โหลดประวัติ", use_container_width=True)
    with col_filter:
        search_keyword = st.text_input("🔎 ค้นหาชื่อบริษัท / ผู้ตรวจสอบ / เลขทะเบียน", key="history_search")

    if refresh or st.session_state.get("history_loaded"):
        st.session_state["history_loaded"] = True

        try:
            resp = requests.get(HISTORY_URL, timeout=30)
            resp.raise_for_status()
            history_items = resp.json()  # list of records
        except requests.exceptions.ConnectionError:
            st.error("ไม่สามารถเชื่อมต่อ API ได้")
            history_items = []
        except Exception as e:
            st.error(f"เกิดข้อผิดพลาด: {e}")
            history_items = []

        if not history_items:
            st.info("ยังไม่มีประวัติการตรวจสอบ หรือไม่สามารถดึงข้อมูลได้")
        else:
            # filter
            if search_keyword.strip():
                kw = search_keyword.strip().lower()
                history_items = [
                    h for h in history_items
                    if kw in str(h.get("company_name", "")).lower()
                    or kw in str(h.get("checker_name", "")).lower()
                    or kw in str(h.get("registration_number", "")).lower()
                ]

            # sort newest first
            history_items = sorted(history_items, key=lambda x: x.get("created_at", 0), reverse=True)

            st.markdown(f"พบ **{len(history_items)}** รายการ")
            st.markdown("---")

            for idx, item in enumerate(history_items):
                created_ts  = item.get("created_at", 0)
                created_str = datetime.fromtimestamp(created_ts).strftime("%d/%m/%Y %H:%M") if created_ts else "—"

                result      = item.get("result", {})
                all_issues  = (
                    result.get("dbd_validation", [])
                    + result.get("document_comparison", [])
                    + result.get("contract_internal_consistency", [])
                )
                counts      = count_severity(all_issues)
                overall     = result.get("summary", {}).get("overall_risk", "—")
                risk_class  = {"สูง": "risk-high", "ปานกลาง": "risk-medium", "ต่ำ": "risk-low"}.get(overall, "")

                company     = item.get("company_name", "—")
                checker     = item.get("checker_name", "—")
                reg_num     = item.get("registration_number", "—")
                q_file      = item.get("quotation_filename", "—")
                c_file      = item.get("contract_filename", "—")

                with st.expander(
                    f"🏢 {company}  |  ตรวจโดย {checker}  |  {created_str}  "
                    f"  🔴{counts['high']}  🟠{counts['medium']}  🟡{counts['low']}",
                    expanded=False
                ):
                    info_col1, info_col2 = st.columns(2)
                    with info_col1:
                        st.markdown(f"**ชื่อบริษัท:** {company}")
                        st.markdown(f"**เลขทะเบียน:** {reg_num}")
                        st.markdown(f"**ผู้ตรวจสอบ:** {checker}")
                    with info_col2:
                        st.markdown(f"**วันที่ตรวจ:** {created_str}")
                        st.markdown(f"**ใบเสนอราคา:** {q_file}")
                        st.markdown(f"**สัญญา:** {c_file}")

                    st.markdown(f"**ความเสี่ยงรวม:** <span class='{risk_class}'>{overall}</span>", unsafe_allow_html=True)
                    st.markdown("---")

                    if result:
                        render_result(result)
                    else:
                        st.warning("ไม่มีข้อมูลผลการตรวจสอบ")
    else:
        st.info("กดปุ่ม 'โหลดประวัติ' เพื่อดูประวัติการตรวจสอบ")