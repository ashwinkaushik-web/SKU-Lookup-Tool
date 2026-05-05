"""
SKU Lookup Tool — Pattern
Streamlit Cloud | Key-pair auth | No login needed
"""

import streamlit as st
import pandas as pd
import snowflake.connector
import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(page_title="SKU Lookup Tool — Pattern", page_icon="⚡", layout="wide")

# ══════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════
st.markdown("""
<style>
    .main-header {display:flex;align-items:center;gap:16px;margin-bottom:4px;}
    .header-icon {width:48px;height:48px;background:linear-gradient(135deg,#3b82f6,#6366f1);border-radius:12px;display:grid;place-items:center;font-size:22px;color:#fff;box-shadow:0 0 24px rgba(59,130,246,0.3);flex-shrink:0;}
    .header-title {font-size:28px;font-weight:700;margin:0;}
    .header-sub {font-size:14px;color:#64748b;margin:0;}

    /* Colored metric cards */
    .metric-card {border-radius:12px;padding:18px 22px;border:1px solid rgba(255,255,255,0.06);}
    .metric-card .label {font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;opacity:0.8;}
    .metric-card .value {font-size:32px;font-weight:700;margin-top:2px;}
    .mc-total {background:rgba(59,130,246,0.12);color:#60a5fa;}
    .mc-dno {background:rgba(239,68,68,0.12);color:#f87171;}
    .mc-ship {background:rgba(34,197,94,0.12);color:#4ade80;}
    .mc-noship {background:rgba(245,158,11,0.12);color:#fbbf24;}
    .mc-fba {background:rgba(168,85,247,0.12);color:#c084fc;}
    .mc-active {background:rgba(6,182,212,0.12);color:#22d3ee;}

    /* Missing items */
    .missing-item {display:inline-block;background:rgba(239,68,68,0.15);color:#f87171;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:500;margin:2px 4px;}

    #MainMenu {visibility:hidden;}
    footer {visibility:hidden;}

    /* Sidebar */
    div[data-testid="stSidebar"] {background:rgba(13,17,23,0.97);}
    .sidebar-section {background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:14px;margin-bottom:12px;}
    .sidebar-section h4 {margin:0 0 8px 0;font-size:14px;}
    .sidebar-section p {margin:0;font-size:12px;color:#94a3b8;line-height:1.5;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# Column config: key → friendly name + settings
# ══════════════════════════════════════════════
COLUMN_MAP = {
    "SKU": {"label": "SKU", "default": True},
    "LISTING_ID": {"label": "Listing ID", "default": True},
    "MARKETPLACE": {"label": "Marketplace", "default": True},
    "VENDOR": {"label": "Vendor", "default": True},
    "PRODUCT_NAME": {"label": "Product Name", "default": True},
    "IS_DNO": {"label": "DNO", "default": True},
    "SHIPPABLE_TAG": {"label": "Shippable", "default": True},
    "LISTING_FULFILLMENT_TYPE": {"label": "Fulfillment Type", "default": True},
    "LISTING_TYPE": {"label": "Listing Type", "default": False},
    "ASIN": {"label": "ASIN", "default": True},
    "FNSKU": {"label": "FNSKU", "default": False},
    "MASTER_ID": {"label": "Master ID", "default": False},
    "MPN": {"label": "MPN", "default": False},
    "COMMINGLED_STATUS": {"label": "Commingled", "default": False},
    "IS_ACTIVE": {"label": "Active", "default": True},
    "IS_DISCONTINUED": {"label": "Discontinued", "default": False},
    "UPC": {"label": "UPC", "default": False},
    "EAN": {"label": "EAN", "default": False},
    "CAN_EXPIRE": {"label": "Can Expire", "default": False},
    "WHOLESALE_PRICE": {"label": "Wholesale Price", "default": False},
    "MAP_PRICE": {"label": "MAP Price", "default": False},
    "RETAIL_PRICE": {"label": "Retail Price", "default": False},
    "MSRP_PRICE": {"label": "MSRP Price", "default": False},
    "DNO_NOTE": {"label": "DNO Note", "default": True},
}

BOOL_COLS = {
    "IS_DNO": ("⛔ YES — DNO", "✅ NO"),
    "SHIPPABLE_TAG": ("✅ YES", "⛔ NO"),
    "IS_ACTIVE": ("✅ YES", "❌ NO"),
    "IS_DISCONTINUED": ("⛔ YES", "✅ NO"),
    "CAN_EXPIRE": ("⚠️ YES", "✅ NO"),
}


# ══════════════════════════════════════════════
# Snowflake connection
# ══════════════════════════════════════════════
@st.cache_resource
def get_connection():
    sf = st.secrets["snowflake"]
    pk_pem = sf["private_key"].encode("utf-8")
    pk = serialization.load_pem_private_key(pk_pem, password=None, backend=default_backend())
    pk_bytes = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    conn = snowflake.connector.connect(
        account=sf["account"], user=sf["user"], private_key=pk_bytes,
        warehouse=sf["warehouse"], role=sf["role"],
        database="ANALYTICS_DB", schema="STG_CATALOG",
    )
    conn.cursor().execute(f"USE WAREHOUSE {sf['warehouse']}")
    return conn


def build_query(skus):
    def safe(s): return s.strip().replace("'", "''")
    upper_list = ", ".join(f"UPPER('{safe(s)}')" for s in skus if s.strip())
    return f"""
WITH q1 AS (
    SELECT c.name AS marketplace, par.name AS vendor, a.Listing_MP_Primary_ID AS sku,
        a.LISTING_FULFILLMENT_TYPE AS listing_fulfillment_type, a.LISTING_ID AS listing_id,
        b.MASTER_ID AS master_id, b.MPN AS mpn, a.LISTING_MP_PAGE_ID AS asin,
        a.LISTING_MP_SECONDARY_ID AS fnsku,
        CASE
            WHEN a.LISTING_FULFILLMENT_TYPE <> 'FBA' THEN NULL
            WHEN a.LISTING_MP_SECONDARY_ID = a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = TRUE THEN 'Commingled'
            WHEN a.LISTING_MP_SECONDARY_ID <> a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = FALSE THEN 'NOT Commingled'
            WHEN a.LISTING_MP_SECONDARY_ID <> a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = TRUE THEN 'Amazon not set to commingled, but Pattern flagged'
            WHEN a.LISTING_MP_SECONDARY_ID = a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = FALSE THEN 'Amazon set as commingled, but Pattern flag not on'
            WHEN a.LISTING_MP_SECONDARY_ID IS NULL THEN 'Missing FNSKU for analysis'
            ELSE 'Check'
        END AS commingled_status,
        dno.DNO_NOTE AS dno_note
    FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS a
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PRODUCTS b ON b.ID = a.PRODUCT_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__MARKETPLACES c ON a.MARKETPLACE_ID = c.ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PARTNERS par ON par.ID = b.PARTNER_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__DNO_SETTINGS dno ON dno.ID = a.DNO_SETTING_ID
),
q2 AS (
    SELECT pc.MARKETPLACE_NAME AS marketplace, pc.VENDOR_NAME AS vendor, pc.MARKETPLACE_PRIMARY_ID AS sku,
        pc.FULFILLMENT_TYPE AS listing_fulfillment_type, pc.LISTING_ID AS listing_id,
        pc.LISTING_IS_SHIPABLE AS shippable_tag, pc.LISTING_TYPE AS listing_type,
        pc.IS_ACTIVE AS is_active, pc.IS_DISCONTINUED AS is_discontinued,
        pc.PRODUCT_NAME AS product_name, pc.UPC AS upc, pc.EAN AS ean,
        pc.CAN_EXPIRE AS can_expire,
        pc.FINANCE_APPROVED_WHOLESALE_PRICE_W_CURRENCY AS wholesale_price,
        pc.MAP_W_CURRENCY AS map_price, pc.RETAIL_W_CURRENCY AS retail_price,
        pc.MSRP_W_CURRENCY AS msrp_price
    FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
),
q3 AS (
    SELECT h.LISTING_ID AS listing_id, h.IS_DNO AS is_dno
    FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY h
    WHERE h."DATE" = (SELECT MAX("DATE") FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY)
),
base AS (
    SELECT COALESCE(q2.marketplace, q1.marketplace) AS MARKETPLACE,
        COALESCE(q2.vendor, q1.vendor) AS VENDOR,
        COALESCE(q2.sku, q1.sku) AS SKU,
        COALESCE(q2.listing_fulfillment_type, q1.listing_fulfillment_type) AS LISTING_FULFILLMENT_TYPE,
        COALESCE(q2.listing_id, q1.listing_id) AS LISTING_ID,
        q1.master_id AS MASTER_ID, q1.mpn AS MPN,
        q1.asin AS ASIN, q1.fnsku AS FNSKU,
        q1.commingled_status AS COMMINGLED_STATUS,
        q2.shippable_tag AS SHIPPABLE_TAG,
        q2.listing_type AS LISTING_TYPE,
        COALESCE(q3.is_dno, FALSE) AS IS_DNO,
        q2.is_active AS IS_ACTIVE, q2.is_discontinued AS IS_DISCONTINUED,
        q2.product_name AS PRODUCT_NAME, q2.upc AS UPC, q2.ean AS EAN,
        q2.can_expire AS CAN_EXPIRE, q2.wholesale_price AS WHOLESALE_PRICE,
        q2.map_price AS MAP_PRICE, q2.retail_price AS RETAIL_PRICE,
        q2.msrp_price AS MSRP_PRICE, q1.dno_note AS DNO_NOTE
    FROM q1 FULL OUTER JOIN q2 ON q1.listing_id = q2.listing_id
    LEFT JOIN q3 ON q3.listing_id = COALESCE(q1.listing_id, q2.listing_id)
)
SELECT * FROM base
WHERE UPPER(SKU) IN ({upper_list}) OR UPPER(LISTING_ID) IN ({upper_list})
   OR UPPER(ASIN) IN ({upper_list}) OR UPPER(MPN) IN ({upper_list})
   OR UPPER(MASTER_ID) IN ({upper_list}) OR UPPER(FNSKU) IN ({upper_list})
ORDER BY MARKETPLACE, VENDOR, SKU
"""


def run_lookup(skus):
    conn = get_connection()
    return pd.read_sql(build_query(skus), conn)


def multiselect_filter(df, column, label, key):
    unique_vals = sorted(df[column].dropna().unique().tolist())
    if not unique_vals:
        return df
    selected = st.multiselect(label, options=unique_vals, default=[], key=key, placeholder="All")
    if selected:
        return df[df[column].isin(selected)]
    return df


def bool_multiselect_filter(df, column, label, key):
    selected = st.multiselect(label, options=["YES", "NO"], default=[], key=key, placeholder="All")
    if not selected:
        return df
    conds = []
    if "YES" in selected:
        conds.append(df[column] == True)
    if "NO" in selected:
        conds.append(df[column] != True)
    if len(conds) == 1:
        return df[conds[0]]
    return df[conds[0] | conds[1]]


def find_missing_items(skus, df):
    """Find which pasted items returned no results."""
    found = set()
    for col in ["SKU", "LISTING_ID", "ASIN", "MPN", "MASTER_ID", "FNSKU"]:
        if col in df.columns:
            found.update(df[col].dropna().str.upper().tolist())
    return [s for s in skus if s.upper() not in found]


# ══════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚡ SKU Lookup Tool")
    st.caption("Pattern — Merchandise Planning")

    st.markdown('<div class="sidebar-section"><h4>📖 How to Use</h4><p>'
                '1. Paste SKUs, Listing IDs, ASINs, MPNs, Master IDs, or FNSKUs — one per line<br>'
                '2. Or upload a CSV/Excel file (first column = identifiers)<br>'
                '3. Click Lookup to query Snowflake<br>'
                '4. Use filters to narrow results<br>'
                '5. Export to CSV when done</p></div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>🔍 Supported Lookups</h4><p>'
                '• SKU (Marketplace Primary ID)<br>'
                '• Listing ID<br>'
                '• ASIN<br>'
                '• MPN<br>'
                '• Master ID<br>'
                '• FNSKU</p></div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>ℹ️ Data Info</h4><p>'
                '• DNO date: Latest available<br>'
                '• Data source: Snowflake<br>'
                '• Results are live (not cached)<br>'
                '• Max 500 items per lookup</p></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.caption(f"v2.0 • {datetime.date.today().strftime('%B %Y')}")


# ══════════════════════════════════════════════
# Header
# ══════════════════════════════════════════════
st.markdown('<div class="main-header"><div class="header-icon">⚡</div><div>'
            '<p class="header-title">SKU Lookup Tool</p>'
            '<p class="header-sub">Check DNO, Shippable, Commingled & more — powered by Snowflake</p>'
            '</div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════
# Input + Results in tabs
# ══════════════════════════════════════════════
input_tab, results_tab = st.tabs(["🔍 Search", "📊 Results"])

with input_tab:
    st.markdown("")
    paste_col, upload_col = st.columns(2)

    skus_to_lookup = []

    with paste_col:
        st.markdown("#### ✏️ Paste Items")
        sku_text = st.text_area(
            "Enter SKUs / Listing IDs / ASINs / MPNs / Master IDs / FNSKUs",
            placeholder="One item per line, e.g.\nUK-BOSCH-786700-COM\nL0NC2POW\nB0BXT6YCHK",
            height=220, label_visibility="collapsed",
        )
        if sku_text.strip():
            skus_to_lookup = [s.strip() for s in sku_text.strip().split("\n") if s.strip()]
        st.caption(f"{len(skus_to_lookup)} item(s) entered • Max 500")

    with upload_col:
        st.markdown("#### 📁 Upload File")
        uploaded_file = st.file_uploader(
            "CSV or Excel (first column = SKUs)",
            type=["csv", "xlsx", "xls"],
            label_visibility="collapsed",
        )
        if uploaded_file:
            try:
                if uploaded_file.name.lower().endswith(".csv"):
                    upload_df = pd.read_csv(uploaded_file, dtype=str)
                else:
                    upload_df = pd.read_excel(uploaded_file, dtype=str, engine="openpyxl")
                skus_to_lookup = upload_df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
                skus_to_lookup = [s for s in skus_to_lookup if s]
                st.success(f"📎 Loaded **{len(skus_to_lookup)}** items from `{uploaded_file.name}`")
            except Exception as e:
                st.error(f"Failed to read file: {e}")

    if skus_to_lookup:
        if len(skus_to_lookup) > 500:
            st.warning("⚠️ Max 500 items. Only first 500 processed.")
            skus_to_lookup = skus_to_lookup[:500]

        if st.button("🔍 Lookup", type="primary", use_container_width=True):
            with st.spinner("Querying Snowflake..."):
                try:
                    df = run_lookup(skus_to_lookup)
                    if df.empty:
                        st.warning("No results found.")
                        st.session_state["results_df"] = pd.DataFrame()
                    else:
                        st.session_state["results_df"] = df
                        st.session_state["skus_count"] = len(skus_to_lookup)
                        st.session_state["skus_list"] = skus_to_lookup
                        st.success(f"✅ Found **{len(df)}** results! Switch to the **Results** tab to view.")
                except Exception as e:
                    st.error(f"Query failed: {e}")

with results_tab:
    if "results_df" not in st.session_state or st.session_state.get("results_df", pd.DataFrame()).empty:
        st.info("👈 Enter items in the **Search** tab and click **Lookup** to see results here.")
    else:
        df = st.session_state["results_df"].copy()
        skus_count = st.session_state.get("skus_count", 0)
        skus_list = st.session_state.get("skus_list", [])

        # ── Missing items ──
        missing = find_missing_items(skus_list, df)
        if missing:
            with st.expander(f"⚠️ {len(missing)} item(s) returned no results — click to see", expanded=False):
                missing_html = "".join(f'<span class="missing-item">{m}</span>' for m in missing)
                st.markdown(missing_html, unsafe_allow_html=True)
                st.caption("These items were not found in any lookup field (SKU, Listing ID, ASIN, MPN, Master ID, FNSKU).")

        # ── Colored summary cards ──
        dno_count = int((df["IS_DNO"] == True).sum()) if "IS_DNO" in df.columns else 0
        shippable_count = int((df["SHIPPABLE_TAG"] == True).sum()) if "SHIPPABLE_TAG" in df.columns else 0
        fba_count = int((df["LISTING_FULFILLMENT_TYPE"].str.upper() == "FBA").sum()) if "LISTING_FULFILLMENT_TYPE" in df.columns else 0
        active_count = int((df["IS_ACTIVE"] == True).sum()) if "IS_ACTIVE" in df.columns else 0

        mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
        with mc1:
            st.markdown(f'<div class="metric-card mc-total"><div class="label">Total</div><div class="value">{len(df)}</div></div>', unsafe_allow_html=True)
        with mc2:
            st.markdown(f'<div class="metric-card mc-dno"><div class="label">DNO = True</div><div class="value">{dno_count}</div></div>', unsafe_allow_html=True)
        with mc3:
            st.markdown(f'<div class="metric-card mc-ship"><div class="label">Shippable</div><div class="value">{shippable_count}</div></div>', unsafe_allow_html=True)
        with mc4:
            st.markdown(f'<div class="metric-card mc-noship"><div class="label">Not Shippable</div><div class="value">{len(df) - shippable_count}</div></div>', unsafe_allow_html=True)
        with mc5:
            st.markdown(f'<div class="metric-card mc-fba"><div class="label">FBA</div><div class="value">{fba_count}</div></div>', unsafe_allow_html=True)
        with mc6:
            st.markdown(f'<div class="metric-card mc-active"><div class="label">Active</div><div class="value">{active_count}</div></div>', unsafe_allow_html=True)

        st.markdown("")

        # ── Filters ──
        st.markdown("### 🔽 Filters")
        search_text = st.text_input("🔍 Search across all fields", placeholder="Type to search...", key="search_all")

        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        filtered = df.copy()
        with r1c1:
            filtered = multiselect_filter(filtered, "MARKETPLACE", "Marketplace", "f_mp")
        with r1c2:
            filtered = multiselect_filter(filtered, "VENDOR", "Vendor", "f_vn")
        with r1c3:
            filtered = bool_multiselect_filter(filtered, "IS_DNO", "DNO", "f_dno")
        with r1c4:
            filtered = bool_multiselect_filter(filtered, "SHIPPABLE_TAG", "Shippable", "f_ship")

        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
        with r2c1:
            filtered = multiselect_filter(filtered, "LISTING_FULFILLMENT_TYPE", "Fulfillment Type", "f_ff")
        with r2c2:
            filtered = multiselect_filter(filtered, "LISTING_TYPE", "Listing Type", "f_lt")
        with r2c3:
            filtered = multiselect_filter(filtered, "COMMINGLED_STATUS", "Commingled", "f_cm")
        with r2c4:
            if "IS_ACTIVE" in filtered.columns:
                filtered = bool_multiselect_filter(filtered, "IS_ACTIVE", "Active", "f_active")

        r3c1, r3c2, r3c3, r3c4 = st.columns(4)
        with r3c1:
            if "IS_DISCONTINUED" in filtered.columns:
                filtered = bool_multiselect_filter(filtered, "IS_DISCONTINUED", "Discontinued", "f_disc")
        with r3c2:
            if "CAN_EXPIRE" in filtered.columns:
                filtered = bool_multiselect_filter(filtered, "CAN_EXPIRE", "Can Expire", "f_expire")

        if search_text.strip():
            mask = filtered.astype(str).apply(
                lambda row: row.str.contains(search_text.strip(), case=False).any(), axis=1
            )
            filtered = filtered[mask]

        st.caption(f"Showing **{len(filtered)}** of **{len(df)}** results")

        # ── Column visibility with friendly names ──
        available_cols = [k for k in COLUMN_MAP if k in filtered.columns]
        default_cols = [k for k in available_cols if COLUMN_MAP[k]["default"]]
        friendly_options = {COLUMN_MAP[k]["label"]: k for k in available_cols}

        with st.expander("👁 Toggle Columns"):
            selected_friendly = st.multiselect(
                "Choose columns to display",
                options=[COLUMN_MAP[k]["label"] for k in available_cols],
                default=[COLUMN_MAP[k]["label"] for k in default_cols],
                key="col_select",
            )
        selected_cols = [friendly_options[f] for f in selected_friendly] if selected_friendly else default_cols

        # ── Format display ──
        display_df = filtered[selected_cols].copy()

        # Rename to friendly names
        rename_map = {k: COLUMN_MAP[k]["label"] for k in selected_cols if k in COLUMN_MAP}
        display_df = display_df.rename(columns=rename_map)

        # Format booleans
        for col_key, (true_label, false_label) in BOOL_COLS.items():
            friendly_name = COLUMN_MAP.get(col_key, {}).get("label", col_key)
            if friendly_name in display_df.columns:
                display_df[friendly_name] = display_df[friendly_name].apply(
                    lambda x: true_label if x == True else false_label
                )

        # Color coding
        dno_friendly = COLUMN_MAP.get("IS_DNO", {}).get("label", "DNO")
        ship_friendly = COLUMN_MAP.get("SHIPPABLE_TAG", {}).get("label", "Shippable")

        def color_rows(row):
            if dno_friendly in row.index and "⛔ YES — DNO" in str(row.get(dno_friendly, "")):
                return ["background-color: rgba(239,68,68,0.08)"] * len(row)
            elif ship_friendly in row.index and "✅ YES" in str(row.get(ship_friendly, "")):
                return ["background-color: rgba(34,197,94,0.05)"] * len(row)
            return [""] * len(row)

        st.dataframe(
            display_df.style.apply(color_rows, axis=1),
            use_container_width=True, hide_index=True,
            height=min(len(display_df) * 38 + 40, 600),
        )

        # ── Export ──
        ex1, ex2 = st.columns(2)
        export_cols = selected_cols
        with ex1:
            st.download_button(
                "⬇️ Export All CSV",
                df[export_cols].rename(columns=rename_map).to_csv(index=False),
                f"sku_lookup_all_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True,
            )
        with ex2:
            st.download_button(
                "⬇️ Export Filtered CSV",
                filtered[export_cols].rename(columns=rename_map).to_csv(index=False),
                f"sku_lookup_filtered_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True,
            )
