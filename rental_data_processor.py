import pandas as pd
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
import os
import hashlib
import re
import calendar
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
    """Clean and standardize address text with proper title casing."""
    if pd.isnull(s):
        return s
    s = str(s).strip().title()
    # Lowercase ordinal suffixes like '9Th' → '9th', '2Nd' → '2nd'
    s = re.sub(r'(\d+)(St|Nd|Rd|Th)\b', lambda m: m.group(1) + m.group(2).lower(), s)
    return s

def deterministic_12_digit(s):
    """Generate a deterministic 12-digit ID from a string using SHA256."""
    h = int(hashlib.sha256(s.encode()).hexdigest(), 16)
    return str(h)[-12:]

def get_csv_file():
    """Open file dialog to select CSV file."""
    root = tk.Tk()
    root.withdraw()
    csv_path = filedialog.askopenfilename(
        title="Select CSV file", 
        filetypes=[("CSV files", "*.csv")]
    )
    if csv_path:
        print(f"Selected: {os.path.basename(csv_path)}")
    return csv_path

def clean_dataframe(df):
    """Clean and process the rental data DataFrame."""
    # Clean address fields
    for col in ['Address', 'Unit']:
        df[col] = df[col].apply(smart_address_title)
    
    # Clean SqFt field (keep as string, just strip whitespace)
    df['SqFt'] = df['SqFt'].astype(str).str.strip()
    
    # Create unique property ID
    property_key = df['Address'] + '|' + df['Unit'] + '|' + df['SqFt']
    df['property_id'] = property_key.apply(deterministic_12_digit)
    
    # Move property_id to first column
    cols = ['property_id'] + [col for col in df.columns if col != 'property_id']
    df = df[cols]
    
    # Remove duplicates based on property_id
    df = df.drop_duplicates(subset='property_id', keep='first').reset_index(drop=True)
    
    return df

def ask_month_year():
    """Open dialog to select month and year for the data."""
    current_month = calendar.month_name[datetime.now().month]
    current_year = str(datetime.now().year)
    
    dialog = tk.Toplevel()
    dialog.title("Select Month and Year")
    dialog.grab_set()

    tk.Label(dialog, text="Month:").grid(row=0, column=0, padx=5, pady=5)
    tk.Label(dialog, text="Year:").grid(row=1, column=0, padx=5, pady=5)

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
    return getattr(dialog, 'result', (current_month, current_year))

def add_month_year_columns(df, month_name, year_str):
    """Add month and year columns to the DataFrame."""
    df = df.copy()
    df['month'] = MONTH_MAP[month_name]
    df['year'] = int(year_str)
    return df

def save_dataframe(df):
    """Save DataFrame to CSV file using file dialog."""
    save_path = filedialog.asksaveasfilename(
        title="Save CSV file",
        defaultextension=".csv",
        initialfile="processed_rental_data.csv",
        filetypes=[("CSV files", "*.csv")]
    )
    if save_path:
        df.to_csv(save_path, index=False)
        print(f"Saved to {os.path.basename(save_path)}")
        return True
    else:
        print("Save cancelled.")
        return False

def main():
    """Main function to process rental data CSV."""
    # Get CSV file from user
    csv_path = get_csv_file()
    if not csv_path:
        print("No file selected. Exiting.")
        return
    
    # Load and clean data
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows from {os.path.basename(csv_path)}")
        
        # Clean the dataframe
        df = clean_dataframe(df)
        print(f"After cleaning and deduplication: {len(df)} rows")
        
        # Get month and year from user
        selected_month, selected_year = ask_month_year()
        
        # Add month/year columns
        df = add_month_year_columns(df, selected_month, selected_year)
        print(f"Added month/year: {selected_month} {selected_year}")
        
        # Save the processed data
        save_dataframe(df)
        
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    main()