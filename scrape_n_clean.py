import os
import re
import time
import hashlib
import calendar
import pandas as pd
import logging
import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from bs4 import BeautifulSoup
import tkinter as tk
from tkinter import ttk, filedialog

# ---------- CONFIGS ----------
HEADLESS = True
WAIT_TIME = 4
LISTINGS_PER_PAGE = 40
BASE_URL = "https://www.apartments.com/apartments-condos/san-diego-county-ca/under-4000/"
LOG_FILE = "scraper_log.txt"
TEST_MODE = True
MAX_UNITS = 10

logging.basicConfig(
    filename=LOG_FILE,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)

MONTH_MAP = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}
MONTHS = list(MONTH_MAP.keys())
YEARS = [str(y) for y in range(2020, 2031)]

# ---------- SCRAPER LOGIC ----------
def init_driver():
    options = Options()
    if HEADLESS:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0")
    service = Service(EdgeChromiumDriverManager().install(), log_output=os.devnull)
    return webdriver.Edge(service=service, options=options)

def extract_low_price(price):
    if pd.isna(price):
        return None
    price = re.sub(r'[^\d\-]', '', str(price))
    return float(price.split('-')[0]) if '-' in price else float(price) if price.isdigit() else None

def extract_amenities(soup):
    labels = soup.select('.amenityLabel') + soup.select('.combinedAmenitiesList li span')
    fee_section = soup.find(id='fees-policies-pets-tab')
    unique_features = soup.select('.uniqueAmenity')
    text = ' '.join(el.get_text(separator=' ').lower().strip() for el in labels + unique_features)
    if fee_section:
        text += ' ' + fee_section.get_text(separator=' ').lower()
    pet_positive_terms = [
        'dogs allowed', 'cats allowed', 'dog friendly', 'cat friendly',
        'pet-friendly', 'pets allowed', 'pet friendly'
    ]
    pet_negative_terms = [
        'no pets', 'pets not allowed', 'not pet friendly', 'no animals'
    ]
    is_pet_friendly = (
        any(term in text for term in pet_positive_terms)
        and not any(term in text for term in pet_negative_terms)
    )
    return {
        'HasWasherDryer': 'washer/dryer' in text or 'in unit washer' in text or 'front loading washer' in text,
        'HasAirConditioning': 'air conditioning' in text or 'central ac' in text or 'central aircon' in text,
        'HasPool': 'pool' in text,
        'HasSpa': 'spa' in text or 'hot tub' in text,
        'HasGym': 'fitness center' in text or 'gym' in text,
        'HasEVCharging': 'ev charging' in text,
        'IsPetFriendly': is_pet_friendly
    }

def scrape_listings(driver):
    all_units = []
    page = 1
    while True:
        url = f"{BASE_URL}{page}/"
        logging.info(f"Scraping page {page}: {url}")
        driver.get(url)
        time.sleep(WAIT_TIME)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        listings = soup.find_all('article')
        if not listings:
            break
        for listing in listings:
            title = listing.find('span', class_='js-placardTitle')
            address = listing.find('div', class_='property-address')
            phone = listing.find('button', class_='phone-link')
            property_url = listing.get('data-url')
            if title and address and property_url:
                try:
                    driver.get(property_url)
                    time.sleep(WAIT_TIME / 2)
                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    unit_containers = detail_soup.find_all('li', class_='unitContainer js-unitContainerV3')
                    rental_type = "Unknown"
                    og_title_tag = detail_soup.find("meta", property="og:title")
                    if og_title_tag and og_title_tag.get("content"):
                        content = og_title_tag["content"].lower()
                        for term in ["house rental", "townhome", "condo", "apartment"]:
                            if term in content:
                                rental_type = term.replace(" rental", "").capitalize()
                    amenities = extract_amenities(detail_soup)
                    for unit in unit_containers:
                        unit_number = unit.find('div', class_='unitColumn column')
                        price = unit.find('div', class_='pricingColumn column')
                        sqft = unit.find('div', class_='sqftColumn column')
                        beds = unit.get('data-beds')
                        baths = unit.get('data-baths')
                        all_units.append({
                            'Property': title.text.strip() if title else "N/A",
                            'Address': address.text.strip() if address else "N/A",
                            'Unit': unit_number.text.strip() if unit_number else "N/A",
                            'Price': price.text.strip() if price else "N/A",
                            'SqFt': sqft.text.strip() if sqft else "N/A",
                            'Beds': beds if beds else "N/A",
                            'Baths': baths if baths else "N/A",
                            'RentalType': rental_type,
                            'Phone': phone.get('phone-data') if phone and phone.has_attr('phone-data') else "N/A",
                            **amenities,
                            'ListingURL': property_url
                        })
                        if TEST_MODE and len(all_units) >= MAX_UNITS:
                            logging.info(f"TEST_MODE: Stopping after {MAX_UNITS} listings.")
                            return pd.DataFrame(all_units)
                except Exception as e:
                    logging.warning(f"Error processing {property_url}: {e}")
        if len(listings) < LISTINGS_PER_PAGE:
            break
        page += 1
    return pd.DataFrame(all_units)

# ---------- CLEANING/DEDUPLICATION/ID GENERATION ----------
def smart_address_title(s):
    if pd.isnull(s):
        return s
    s = str(s).strip().title()
    s = re.sub(r'(\d+)(St|Nd|Rd|Th)\b', lambda m: m.group(1) + m.group(2).lower(), s)
    return s

def deterministic_12_digit(s):
    h = int(hashlib.sha256(s.encode()).hexdigest(), 16)
    return str(h)[-12:]

def extract_city_state(address):
    if pd.isnull(address):
        return pd.Series([np.nan, np.nan])
    # Use re.IGNORECASE so it matches 'CA', 'Ca', 'ca', etc.
    m = re.search(r',\s*([^,]+),\s*([A-Z]{2})\s*\d{5}', address, re.IGNORECASE)
    if m:
        # Return the city and uppercase version of the state
        return pd.Series([m.group(1).strip(), m.group(2).strip().upper()])
    m2 = re.search(r',\s*([A-Z]{2})\s*\d{5}', address, re.IGNORECASE)
    if m2:
        return pd.Series([np.nan, m2.group(1).strip().upper()])
    m3 = re.search(r',\s*(\d{5})$', address)
    if m3:
        return pd.Series([np.nan, np.nan])
    return pd.Series([np.nan, np.nan])

def clean_and_finalize_dataframe(df):
    import numpy as np

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
        lambda row: round(row['Price'] / row['SqFt'], 2)
        if pd.notnull(row['Price']) and pd.notnull(row['SqFt']) and row['SqFt'] > 0 else None,
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

    # Keep amenity columns as boolean for database compatibility
    # No need to convert to 'Yes'/'No' strings since database expects BOOLEAN

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

# ---------- MAIN WORKFLOW ----------
def main():
    start_time = time.time()
    driver = init_driver()
    df = scrape_listings(driver)
    driver.quit()
    if df.empty:
        print("No data collected. File not saved.")
        logging.warning("No data collected. File not saved.")
        return
    # Clean, deduplicate, property_id
    df = clean_and_finalize_dataframe(df)
    # Month/year confirmation (GUI)
    selected_month, selected_year = ask_month_year()
    df = add_month_year_columns(df, selected_month, selected_year)
    
    # Rename columns to match database schema (lowercase)
    column_mapping = {
        'Property': 'property',
        'Address': 'address', 
        'City': 'city',
        'State': 'state',
        'ZipCode': 'zipcode',
        'Phone': 'phone',
        'Unit': 'unit',
        'Beds': 'beds',
        'Baths': 'baths',
        'Beds_Baths': 'beds_baths',
        'SqFt': 'sqft',
        'Price': 'price',
        'PricePerSqFt': 'pricepersqft',
        'RentalType': 'rentaltype',
        'HasWasherDryer': 'haswasherdryer',
        'HasAirConditioning': 'hasairconditioning',
        'HasPool': 'haspool',
        'HasSpa': 'hasspa',
        'HasGym': 'hasgym',
        'HasEVCharging': 'hasevcharging',
        'IsPetFriendly': 'ispetfriendly',
        'ListingURL': 'listingurl'
    }
    df = df.rename(columns=column_mapping)
    
    # Final order for DB loader to match database schema
    final_cols = [
        'property_id', 'property', 'address', 'city', 'state', 'zipcode', 'phone', 'unit',
        'beds', 'baths', 'beds_baths', 'sqft', 'price', 'pricepersqft',
        'rentaltype', 'haswasherdryer', 'hasairconditioning', 'haspool', 'hasspa',
        'hasgym', 'hasevcharging', 'ispetfriendly', 'listingurl', 'month', 'year'
    ]
    for col in final_cols:
        if col not in df.columns:
            if col in ['beds', 'baths', 'sqft', 'price', 'pricepersqft', 'month', 'year']:
                df[col] = 0
            elif col.startswith('has') or col.startswith('is'):
                df[col] = False
            else:
                df[col] = "N/A"
    df = df[final_cols]
    # Save processed CSV (file dialog)
    root = tk.Tk()
    root.withdraw()
    save_path = filedialog.asksaveasfilename(
        title="Save processed CSV for DB import",
        defaultextension=".csv",
        initialfile=f"SD_county_{selected_month}_{selected_year}.csv",
        filetypes=[("CSV files", "*.csv")]
    )
    if save_path:
        df.to_csv(save_path, index=False)
        print(f"\nSaved ready-to-import CSV to: {os.path.basename(save_path)}\n")
    else:
        print("Save cancelled.")
    duration = time.time() - start_time
    minutes, seconds = divmod(duration, 60)
    print(f"Total runtime: {int(minutes)} min {seconds:.2f} sec")

if __name__ == "__main__":
    main()