"""
SKU Lookup Tool — Streamlit Cloud Version
Uses Snowflake key-pair auth via service account. No login needed.
"""

import streamlit as st
import pandas as pd
import snowflake.connector
import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(page_title="SKU Lookup Tool — Pattern", page_icon="⚡", layout="wide")

st.markdown("""
<style>
    .main-header {display:flex;align-items:center;gap:16px;margin-bottom:8px;}
    .header-icon {width:48px;height:48px;background:linear-gradient(135deg,#3b82f6,#6366f1);border-radius:12px;display:grid;place-items:center;font-size:22px;color:#fff;box-shadow:0 0 24px rgba(59,130,246,0.3);flex-shrink:0;}
    .header-title {font-size:28px;font-weight:700;margin:0;}
    .header-sub {font-size:14px;color:#64748b;margin:0;}
    [data-testid="stMetric"] {background:rgba(17,24,39,0.5);border:1px solid rgba(30,41,59,0.5);border-radius:12px;padding:16px 20px;}
    #MainMenu {visibility:hidden;}
    footer {visibility:hidden;}
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_connection():
    """Create Snowflake connection using key-pair auth from Streamlit Secrets."""
    sf = st.secrets["snowflake"]

    # Load private key
    private_key_pem = sf["private_key"].encode("utf-8")
    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=None,
        backend=default_backend(),
    )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        private_key=private_key_bytes,
        warehouse=sf["warehouse"],
        role=sf["role"],
        database="ANALYTICS_DB",
        schema="STG_CATALOG",
    )


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
        END AS commingled_status
    FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS a
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PRODUCTS b ON b.ID = a.PRODUCT_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__MARKETPLACES c ON a.MARKETPLACE_ID = c.ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PARTNERS par ON par.ID = b.PARTNER_ID
),
q2 AS (
    SELECT pc.MARKETPLACE_NAME AS marketplace, pc.VENDOR_NAME AS vendor, pc.MARKETPLACE_PRIMARY_ID AS sku,
        pc.FULFILLMENT_TYPE AS listing_fulfillment_type, pc.LISTING_ID AS listing_id,
        pc.LISTING_IS_SHIPABLE AS shippable_tag, pc.LISTING_TYPE AS listing_type
    FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
),
q3 AS (
    SELECT h.LISTING_ID AS listing_id, h.IS_DNO AS is_dno
    FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY h
    WHERE h."DATE" = (SELECT MAX("DATE") FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY)
),
base AS (
    SELECT COALESCE(q2.marketplace, q1.marketplace) AS MARKETPLACE, COALESCE(q2.vendor, q1.vendor) AS VENDOR,
        COALESCE(q2.sku, q1.sku) AS SKU, COALESCE(q2.listing_fulfillment_type, q1.listing_fulfillment_type) AS LISTING_FULFILLMENT_TYPE,
        COALESCE(q2.listing_id, q1.listing_id) AS LISTING_ID, q1.master_id AS MASTER_ID, q1.mpn AS MPN,
        q1.asin AS ASIN, q1.fnsku AS FNSKU, q1.commingled_status AS COMMINGLED_STATUS,
        q2.shippable_tag AS SHIPPABLE_TAG, q2.listing_type AS LISTING_TYPE,
        COALESCE(q3.is_dno, FALSE) AS IS_DNO
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


# ── Header ──
st.markdown('<div class="main-header"><div class="header-icon">⚡</div><div><p class="header-title">SKU Lookup Tool</p><p class="header-sub">Check DNO, Shippable, Commingled status & more — powered by Snowflake</p></div></div>', unsafe_allow_html=True)
st.markdown("")

# ── Input ──
tab_paste, tab_upload = st.tabs(["✏️ Paste SKUs", "📁 Upload File"])
skus_to_lookup = []

with tab_paste:
    sku_text = st.text_area("Enter SKUs / Listing IDs / ASINs / MPNs / Master IDs / FNSKUs", placeholder="One item per line", height=180)
    if sku_text.strip():
        skus_to_lookup = [s.strip() for s in sku_text.strip().split("\n") if s.strip()]
    st.caption("One item per line. Max 500 items.")

with tab_upload:
    uploaded_file = st.file_uploader("Upload CSV or Excel (first column = SKUs)", type=["csv", "xlsx", "xls"])
    if uploaded_file:
        try:
            if uploaded_file.name.lower().endswith(".csv"): upload_df = pd.read_csv(uploaded_file, dtype=str)
            else: upload_df = pd.read_excel(uploaded_file, dtype=str, engine="openpyxl")
            skus_to_lookup = upload_df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
            skus_to_lookup = [s for s in skus_to_lookup if s]
            st.success(f"📎 Loaded {len(skus_to_lookup)} items from `{uploaded_file.name}`")
        except Exception as e: st.error(f"Failed to read file: {e}")

# ── Lookup ──
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
            except Exception as e: st.error(f"Query failed: {e}")

# ── Results ──
if "results_df" in st.session_state and not st.session_state["results_df"].empty:
    df = st.session_state["results_df"].copy()
    skus_count = st.session_state.get("skus_count", 0)
    st.success(f"Found **{len(df)}** result(s) for **{skus_count}** item(s)")

    dno_count = int((df["IS_DNO"] == True).sum()) if "IS_DNO" in df.columns else 0
    shippable_count = int((df["SHIPPABLE_TAG"] == True).sum()) if "SHIPPABLE_TAG" in df.columns else 0
    fba_count = int((df["LISTING_FULFILLMENT_TYPE"].str.upper() == "FBA").sum()) if "LISTING_FULFILLMENT_TYPE" in df.columns else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total", len(df))
    c2.metric("DNO = True", dno_count)
    c3.metric("Shippable", shippable_count)
    c4.metric("Not Shippable", len(df) - shippable_count)
    c5.metric("FBA", fba_count)

    st.markdown("### 🔽 Filters")
    fc1, fc2, fc3, fc4, fc5, fc6 = st.columns(6)
    with fc1: sel_mp = st.selectbox("Marketplace", ["All"] + sorted(df["MARKETPLACE"].dropna().unique().tolist()))
    with fc2: sel_vendor = st.selectbox("Vendor", ["All"] + sorted(df["VENDOR"].dropna().unique().tolist()))
    with fc3: sel_dno = st.selectbox("DNO", ["All", "YES", "NO"])
    with fc4: sel_ship = st.selectbox("Shippable", ["All", "YES", "NO"])
    with fc5: sel_ff = st.selectbox("Fulfillment", ["All"] + sorted(df["LISTING_FULFILLMENT_TYPE"].dropna().unique().tolist()))
    with fc6: search_text = st.text_input("🔍 Search", placeholder="Search any field...")

    filtered = df.copy()
    if sel_mp != "All": filtered = filtered[filtered["MARKETPLACE"] == sel_mp]
    if sel_vendor != "All": filtered = filtered[filtered["VENDOR"] == sel_vendor]
    if sel_dno == "YES": filtered = filtered[filtered["IS_DNO"] == True]
    elif sel_dno == "NO": filtered = filtered[filtered["IS_DNO"] != True]
    if sel_ship == "YES": filtered = filtered[filtered["SHIPPABLE_TAG"] == True]
    elif sel_ship == "NO": filtered = filtered[filtered["SHIPPABLE_TAG"] != True]
    if sel_ff != "All": filtered = filtered[filtered["LISTING_FULFILLMENT_TYPE"] == sel_ff]
    if search_text.strip():
        mask = filtered.astype(str).apply(lambda row: row.str.contains(search_text.strip(), case=False).any(), axis=1)
        filtered = filtered[mask]

    st.caption(f"Showing **{len(filtered)}** of **{len(df)}** results")

    display_cols = ["SKU","LISTING_ID","MARKETPLACE","VENDOR","IS_DNO","SHIPPABLE_TAG","LISTING_FULFILLMENT_TYPE","LISTING_TYPE","ASIN","FNSKU","MASTER_ID","MPN","COMMINGLED_STATUS"]
    display_cols = [c for c in display_cols if c in filtered.columns]

    with st.expander("👁 Toggle Columns"):
        selected_cols = st.multiselect("Choose columns to display", options=display_cols, default=display_cols)
    if not selected_cols: selected_cols = display_cols

    display_df = filtered[selected_cols].copy()
    if "IS_DNO" in display_df.columns:
        display_df["IS_DNO"] = display_df["IS_DNO"].apply(lambda x: "⛔ YES — DNO" if x == True else "✅ NO")
    if "SHIPPABLE_TAG" in display_df.columns:
        display_df["SHIPPABLE_TAG"] = display_df["SHIPPABLE_TAG"].apply(lambda x: "✅ YES" if x == True else "⛔ NO")

    def color_rows(row):
        if "IS_DNO" in row.index and row["IS_DNO"] == "⛔ YES — DNO":
            return ["background-color: rgba(239,68,68,0.08)"] * len(row)
        elif "SHIPPABLE_TAG" in row.index and row["SHIPPABLE_TAG"] == "✅ YES":
            return ["background-color: rgba(34,197,94,0.05)"] * len(row)
        return [""] * len(row)

    st.dataframe(display_df.style.apply(color_rows, axis=1), use_container_width=True, hide_index=True, height=min(len(display_df)*38+40, 600))

    col_ex1, col_ex2 = st.columns(2)
    with col_ex1:
        st.download_button("⬇️ Export All CSV", df[display_cols].to_csv(index=False), f"sku_lookup_all_{datetime.date.today().isoformat()}.csv", "text/csv", use_container_width=True)
    with col_ex2:
        st.download_button("⬇️ Export Filtered CSV", filtered[display_cols].to_csv(index=False), f"sku_lookup_filtered_{datetime.date.today().isoformat()}.csv", "text/csv", use_container_width=True)
