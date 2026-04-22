# SKU Lookup Tool — Pattern

A Streamlit web app to check **DNO status**, **Shippable tag**, **Commingled status**, and other product attributes by querying Snowflake.

## Features
- Paste individual SKUs or upload CSV/Excel files
- Searches across SKU, Listing ID, ASIN, MPN, Master ID, and FNSKU
- All marketplaces and fulfillment types included
- DNO uses latest available date automatically
- Real-time filters for Marketplace, Vendor, DNO, Shippable, Fulfillment
- Column visibility toggle
- Export all or filtered results to CSV
- Each user logs in with their own Snowflake credentials

## Deploy to Streamlit Cloud
1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Sign in with your GitHub account
4. Click **New app** → select this repo → set main file to `app.py`
5. Click **Deploy**

## Run Locally
```bash
pip install -r requirements.txt
streamlit run app.py
```
