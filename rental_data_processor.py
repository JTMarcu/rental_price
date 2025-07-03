import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog
import os
import hashlib
import re
import calendar
import numpy as np
from datetime import datetime

# Constants
MONTH_MAP = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}
MONTHS = list(MONTH_MAP.keys())
YEARS = [str(y) for y in range(2020, 2031)]

def smart_address_title(s):
    """Standardize address/unit formatting."""
    if pd.isnull(s):
        return s
    s = str(s).strip().title()
    s = re.sub(r'(\d+)(St|Nd|Rd|Th)\b', lambda m: m.group(1) + m.group(2).lower(), s)
    return s

def deterministic_12_digit(s):
    """Generate deterministic 12-digit property_id."""
    h = int(hashlib.sha256(s.encode()).hexdigest(), 16)
    return str(h)[-12:]

def extract_city_state(address):
    """
    Extract City and State robustly from US-style addresses.
    Handles:
      - "123 Main St, San Diego, CA 92101"
      - "123 Main St, CA 92101"
      - "123 Main St, 92101"
      - and missing values gracefully.
    """
    if pd.isnull(address):
        return pd.Series([np.nan, np.nan])
    # Try full match with city and state
    m = re.search(r',\s*([^,]+),\s*([A-Z]{2})\s*\d{5}', address)
    if m:
        return pd.Series([m.group(1).strip(), m.group(2).strip()])
    # Try just state and zip
    m2 = re.search(r',\s*([A-Z]{2})\s*\d{5}', address)
    if m2:
        return pd.Series([np.nan, m2.group(1).strip()])
    # Only zip at end, nothing else
    m3 = re.search(r',\s*(\d{5})$', address)
    if m3:
        return pd.Series([np.nan, np.nan])
    return pd.Series([np.nan, np.nan])

def clean_and_finalize_dataframe(df):
    # Standardize text fields
    for col in ['Address', 'Unit']:
        if col in df.columns:
            df[col] = df[col].apply(smart_address_title)
    # Clean numeric fields
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    if 'SqFt' in df.columns:
        df['SqFt'] = pd.to_numeric(df['SqFt'].astype(str).str.replace(',', '').str.extract(r'(\d+)', expand=False), errors='coerce')
    if 'Beds' in df.columns:
        df['Beds'] = pd.to_numeric(df['Beds'], errors='coerce')
    if 'Baths' in df.columns:
        df['Baths'] = pd.to_numeric(df['Baths'], errors='coerce')
    # Extract ZipCode
    df['ZipCode'] = df['Address'].str.extract(r'(\d{5})(?!.*\d{5})')
    # Extract City and State ONLY if needed
    if not ('City' in df.columns and 'State' in df.columns):
        df[['City', 'State']] = df['Address'].apply(extract_city_state)
    else:
        # If both exist but City is all NaN or empty, extract
        if df['City'].isnull().all() or df['City'].eq('').all():
            df[['City', 'State']] = df['Address'].apply(extract_city_state)
    # Calculate price per sqft
    df['PricePerSqFt'] = df.apply(
        lambda row: round(row['Price'] / row['SqFt'], 2) if pd.notnull(row['Price']) and pd.notnull(row['SqFt']) and row['SqFt'] > 0 else None,
        axis=1
    )
    # Beds_Baths combined field
    df['Beds_Baths'] = df.apply(
        lambda row: f"{int(row['Beds']) if pd.notnull(row['Beds']) else 'N/A'} Bed / {int(row['Baths']) if pd.notnull(row['Baths']) else 'N/A'} Bath",
        axis=1
    )
    # Deterministic property_id
    property_key = df['Address'].fillna('') + '|' + df['Unit'].fillna('') + '|' + df['SqFt'].fillna('').astype(str)
    df['property_id'] = property_key.apply(deterministic_12_digit)
    # Move property_id to first column
    cols = ['property_id'] + [col for col in df.columns if col != 'property_id']
    df = df[cols]
    # Remove duplicates on property_id
    df = df.drop_duplicates(subset='property_id', keep='first').reset_index(drop=True)
    return df

def ask_month_year():
    root = tk.Tk()
    root.withdraw()
    dialog = tk.Toplevel()
    dialog.title("Select Month and Year")
    dialog.grab_set()
    tk.Label(dialog, text="Month:").grid(row=0, column=0, padx=5, pady=5)
    tk.Label(dialog, text="Year:").grid(row=1, column=0, padx=5, pady=5)
    current_month = calendar.month_name[datetime.now().month]
    current_year = str(datetime.now().year)
    month_var = tk.StringVar(value=current_month)
    year_var = tk.StringVar(value=current_year)
    month_cb = ttk.Combobox(dialog, textvariable=month_var, values=MONTHS, state="readonly")
    year_cb = ttk.Combobox(dialog, textvariable=year_var, values=YEARS, state="readonly")
    month_cb.grid(row=0, column=1, padx=5, pady=5)
    year_cb.grid(row=1, column=1, padx=5, pady=5)
    def on_ok():
        dialog.result = (month_cb.get(), year_cb.get())
        dialog.destroy()
    ok_btn = tk.Button(dialog, text="OK", command=on_ok)
    ok_btn.grid(row=2, column=0, columnspan=2, pady=10)
    dialog.wait_window()
    root.destroy()
    return getattr(dialog, 'result', (current_month, current_year))

def add_month_year_columns(df, month_name, year_str):
    df = df.copy()
    df['month'] = MONTH_MAP[month_name]
    df['year'] = int(year_str)
    return df

def save_dataframe(df, month_name, year_str):
    save_path = filedialog.asksaveasfilename(
        title="Save processed CSV for DB import",
        defaultextension=".csv",
        initialfile=f"SD_county_{month_name}_{year_str}.csv",
        filetypes=[("CSV files", "*.csv")]
    )
    if save_path:
        df.to_csv(save_path, index=False)
        print(f"\nSaved ready-to-import CSV to: {os.path.basename(save_path)}\n")
    else:
        print("Save cancelled.")

def main():
    root = tk.Tk()
    root.withdraw()
    csv_path = filedialog.askopenfilename(
        title="Select original rental CSV file",
        filetypes=[("CSV files", "*.csv")]
    )
    if not csv_path:
        print("No file selected. Exiting.")
        return
    df = pd.read_csv(csv_path)
    df = clean_and_finalize_dataframe(df)
    selected_month, selected_year = ask_month_year()
    df = add_month_year_columns(df, selected_month, selected_year)
    # Final column order to match new standard
    final_cols = [
        'property_id', 'Property', 'Address', 'City', 'State', 'ZipCode', 'Phone', 'Unit',
        'Beds', 'Baths', 'Beds_Baths', 'SqFt', 'Price', 'PricePerSqFt',
        'RentalType', 'HasWasherDryer', 'HasAirConditioning', 'HasPool', 'HasSpa',
        'HasGym', 'HasEVCharging', 'IsPetFriendly', 'ListingURL', 'month', 'year'
    ]
    # Only keep columns that exist in the DataFrame (handles older CSVs)
    df = df[[col for col in final_cols if col in df.columns]]
    # Debug: print addresses missing city/state
    if df['City'].isnull().any() or df['State'].isnull().any():
        print("Some addresses are missing City or State. First few examples:")
        print(df[df['City'].isnull() | df['State'].isnull()][['Address', 'City', 'State']].head(10))
    save_dataframe(df, selected_month, selected_year)

if __name__ == "__main__":
    main()