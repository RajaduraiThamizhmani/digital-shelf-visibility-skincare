import pandas as pd
import numpy as np
import os
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import re

# === CONFIG ===
OUTPUT_DIR = "output"
MERGED_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "merged_visibility.csv")
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1gZsMveLK1rDLP0_SX3no1za9_IhAlLWCPy3fDmYEKO0/edit#gid=0"
CREDENTIALS_FILE = "digitalshelf-461704-29aabf6b8cf1.json"
YOUR_EMAIL = "devikarajadurai@gmail.com"
UNIQUE_BRANDS_FILE = r"D://Python_Projects//Digital Shelf Visibility & Price Intel//scraper//output//extracted_brand_names1.csv"

# Load unique brand names
def load_unique_brands(file_path):
    try:
        df_brands = pd.read_csv(file_path)
        brand_col = 'brand_name' if 'brand_name' in df_brands.columns else df_brands.columns[0]
        brands = df_brands[brand_col].dropna().str.lower().str.strip().unique().tolist()
        return brands
    except Exception as e:
        print(f"❌ Failed to load unique brand names file: {e}")
        return []

unique_brands_list = []

# Extract brand name
def extract_brand_name(product_name):
    if not isinstance(product_name, str) or product_name.strip() == "":
        return np.nan
    product_name_lower = product_name.lower()
    for brand in unique_brands_list:
        pattern = r"^(the\s+)?" + re.escape(brand)
        if re.match(pattern, product_name_lower):
            return brand
    brand_match = re.match(r"^[a-zA-Z&\.\-']+", product_name_lower)
    if brand_match:
        return brand_match.group(0).strip()
    return np.nan

# Load data from each platform
def load_data(platform_name, file_name):
    try:
        df = pd.read_csv(file_name)
        df["platform"] = platform_name
        return df
    except Exception as e:
        print(f"❌ Could not load {platform_name}: {e}")
        return pd.DataFrame()

# Clean fields
def clean_and_normalize(df):
    if 'product_name' in df.columns:
        df['product_name'] = (
            df['product_name']
            .astype(str)
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)
            .str.replace(r'[^\x00-\x7F]+', '', regex=True)
            .str.lower()
            .str.slice(0, 100)
        )
        df = df[df['product_name'].astype(bool)]
        df['brand_name'] = df['product_name'].apply(extract_brand_name)

    if 'stock_status' in df.columns:
        df['stock_status'] = (
            df['stock_status']
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({
                'in stock': 'in_stock',
                'available': 'in_stock',
                'out of stock': 'out_of_stock',
                'unavailable': 'out_of_stock',
                'not available': 'out_of_stock',
                ' ': 'out_of_stock',
                'N/A': 'out_of_stock'
            })
        )

    if 'discount_percent' in df.columns:
        df['discount_percent'] = (
            df['discount_percent']
            .astype(str)
            .str.extract(r'(\d+\.?\d*)')  # Only numeric value from e.g. "(31% OFF)"
            .replace('', np.nan)
            .astype(float)
        )

    return df

# Upload to Google Sheet by appending rows
def upload_to_gsheet_append(df_new, credentials_file, share_with_email):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open_by_url(GOOGLE_SHEET_URL)
        print("📄 Opened sheet by URL.")
    except Exception as e:
        print(f"❌ Failed to open sheet by URL: {e}")
        return

    try:
        sheet.share(share_with_email, perm_type="user", role="writer")
        print(f"🔗 Shared with {share_with_email}")
    except Exception as e:
        print(f"⚠️ Couldn't share sheet: {e}")

    worksheet = sheet.sheet1

    try:
        data_existing = worksheet.get_all_records()
        df_existing = pd.DataFrame(data_existing)
        print(f"📊 Existing rows: {len(df_existing)} | New rows: {len(df_new)}")

        # Align columns
        common_columns = list(set(df_existing.columns).intersection(set(df_new.columns)))
        df_new = df_new[common_columns]

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

    except Exception as e:
        print(f"⚠️ Could not read existing data: {e}")
        df_combined = df_new

    # Clear + Write back
    worksheet.clear()
    set_with_dataframe(worksheet, df_combined)

    print(f"✅ Appended total {len(df_new)} new rows to Google Sheet.")
    print(f"🔗 {sheet.url}")

# Merge and upload logic
def merge_all_and_upload():
    global unique_brands_list
    unique_brands_list = load_unique_brands(UNIQUE_BRANDS_FILE)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = {
        "Amazon": os.path.join(OUTPUT_DIR, "amazon_output.csv"),
        "Flipkart": os.path.join(OUTPUT_DIR, "flipkart_output.csv"),
        "Nykaa": os.path.join(OUTPUT_DIR, "nykaa_output.csv"),
        "Myntra": os.path.join(OUTPUT_DIR, "myntra_output.csv"),
    }

    combined_df = pd.concat(
        [load_data(name, path) for name, path in files.items()],
        ignore_index=True
    )

    if not combined_df.empty:
        if 'url' in combined_df.columns:
            combined_df = combined_df.drop(columns=['url'])

        combined_df = clean_and_normalize(combined_df)
        combined_df['date'] = datetime.today().strftime('%Y-%m-%d')

        combined_df.to_csv(MERGED_OUTPUT_FILE, index=False)
        print(f"✅ Merged data saved to {MERGED_OUTPUT_FILE}")

        upload_to_gsheet_append(combined_df, CREDENTIALS_FILE, YOUR_EMAIL)
    else:
        print("⚠️ No data found to merge or upload.")

# Run script
if __name__ == "__main__":
    merge_all_and_upload()
