
# apartments_scraper.py (final version with reordered columns)
import os
import time
import re
import logging
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager

HEADLESS = True
WAIT_TIME = 4
BASE_URL = "https://www.apartments.com/apartments-condos/san-diego-county-ca/under-4000/"
LOG_FILE = "scraper_log.txt"

logging.basicConfig(
    filename=LOG_FILE,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)

def init_driver():
    options = Options()
    if HEADLESS:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0")
    service = Service(EdgeChromiumDriverManager().install(), log_output=os.devnull)
    return webdriver.Edge(service=service, options=options)

def extract_amenities(soup):
    labels = soup.select('.amenityLabel') + soup.select('.combinedAmenitiesList li span')
    all_text = ' '.join(el.get_text(separator=' ').lower() for el in labels)
    logging.debug("Extracted amenities text: %s", all_text)
    return {
        'HasWasherDryer': 'washer/dryer' in all_text or 'in unit washer' in all_text,
        'HasAirConditioning': 'air conditioning' in all_text,
        'HasPool': 'pool' in all_text,
        'HasSpa': 'spa' in all_text or 'hot tub' in all_text,
        'AllowsDogs': 'dog friendly' in all_text or 'dogs allowed' in all_text,
        'AllowsCats': 'cat friendly' in all_text or 'cats allowed' in all_text,
        'HasEVCharging': 'ev charging' in all_text,
        'HasGym': 'fitness center' in all_text or 'gym' in all_text
    }

def extract_pet_fees(soup):
    pet_info = {}
    for row in soup.select("#feesSection .feeItem"):
        label = row.select_one(".feeName")
        value = row.select_one(".feeValue")
        if label and value:
            ltext = label.get_text(strip=True).lower()
            vtext = value.get_text(strip=True).replace("$", "")
            try:
                vnum = int(re.search(r"\d+", vtext).group())
            except:
                continue
            if "dog" in ltext and "rent" in ltext:
                pet_info['DogRent'] = vnum
            elif "cat" in ltext and "rent" in ltext:
                pet_info['CatRent'] = vnum
            elif "dog" in ltext and "deposit" in ltext:
                pet_info['DogDeposit'] = vnum
            elif "cat" in ltext and "deposit" in ltext:
                pet_info['CatDeposit'] = vnum
            elif "pet rent" in ltext:
                pet_info.setdefault('DogRent', vnum)
                pet_info.setdefault('CatRent', vnum)
            elif "pet deposit" in ltext or "one time fee" in ltext:
                pet_info.setdefault('DogDeposit', vnum)
                pet_info.setdefault('CatDeposit', vnum)
    return pet_info

def extract_parking(soup):
    text = soup.get_text(separator=' ').lower()
    if 'garage' in text:
        return 'garage'
    elif 'covered' in text:
        return 'covered'
    elif 'surface lot' in text:
        return 'surface lot'
    elif 'assigned' in text:
        return 'assigned'
    return 'N/A'

def extract_storage_fee(soup):
    try:
        fee_row = soup.find(string=re.compile("storage", re.I))
        if fee_row:
            fee_div = fee_row.find_parent("div")
            return int(re.search(r'\$\d+', fee_div.text).group().replace("$", ""))
    except:
        return None

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
            units = parse_listing(listing, driver)
            all_units.extend(units)
        page += 1
    return pd.DataFrame(all_units)

def parse_listing(listing, driver):
    units = []
    title = listing.find('span', class_='js-placardTitle')
    address = listing.find('div', class_='property-address')
    phone = listing.find('button', class_='phone-link')
    property_url = listing.get('data-url')
    if not (title and address and property_url): return []

    try:
        driver.get(property_url)
        time.sleep(WAIT_TIME / 2)
        detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
        unit_blocks = detail_soup.select('li.unitContainer')

        rental_type = "Unknown"
        og_title = detail_soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            content = og_title["content"].lower()
            for t in ["house", "townhome", "condo", "apartment"]:
                if t in content:
                    rental_type = t.capitalize()
                    break

        base_data = {
            'Property': title.text.strip(),
            'Address': address.text.strip(),
            'Phone': phone.get('phone-data') if phone and phone.has_attr('phone-data') else "N/A",
            'RentalType': rental_type,
            **extract_amenities(detail_soup),
            **extract_pet_fees(detail_soup),
            'ParkingType': extract_parking(detail_soup),
            'StorageFee': extract_storage_fee(detail_soup),
            'ListingURL': property_url
        }

        for unit in unit_blocks:
            units.append(parse_unit(unit, base_data))
    except Exception as e:
        logging.warning("Error on property: %s | %s", property_url, e)
    return units

def parse_unit(unit, base_data):
    return {
        **base_data,
        'Unit': unit.find('div', class_='unitColumn column').text.strip() if unit.find('div', class_='unitColumn column') else "N/A",
        'Price': unit.find('div', class_='pricingColumn column').text.strip() if unit.find('div', class_='pricingColumn column') else "N/A",
        'SqFt': unit.find('div', class_='sqftColumn column').text.strip() if unit.find('div', class_='sqftColumn column') else "N/A",
        'Beds': unit.get('data-beds', "N/A"),
        'Baths': unit.get('data-baths', "N/A")
    }

def extract_low_price(price):
    if pd.isna(price): return None
    price = re.sub(r'[^\d\-]', '', str(price))
    return float(price.split('-')[0]) if '-' in price else float(price) if price.isdigit() else None

def clean_data(df):
    df['Price'] = df['Price'].apply(extract_low_price)
    df['SqFt'] = pd.to_numeric(df['SqFt'].astype(str).str.replace(',', '').str.extract(r'(\d+)', expand=False), errors='coerce')
    df['Beds'] = pd.to_numeric(df['Beds'], errors='coerce')
    df['Baths'] = pd.to_numeric(df['Baths'], errors='coerce')
    df['ZipCode'] = df['Address'].str.extract(r'(\d{5})(?!.*\d{5})')
    city_state = df['Address'].str.extract(r',\s*([^,]+),\s*([A-Z]{2})\s*\d{5}')
    df['City'] = city_state[0].str.strip()
    df['State'] = city_state[1].str.strip()
    df['PricePerSqFt'] = df.apply(lambda row: round(row['Price'] / row['SqFt'], 2)
                                  if pd.notnull(row['Price']) and pd.notnull(row['SqFt']) and row['SqFt'] > 0 else None, axis=1)
    df['Beds_Baths'] = df.apply(lambda row: f"{int(row['Beds']) if pd.notnull(row['Beds']) else 'N/A'} Bed / {int(row['Baths']) if pd.notnull(row['Baths']) else 'N/A'} Bath", axis=1)

    final_order = [
        'Property', 'Address', 'City', 'State', 'ZipCode', 'Phone',
        'Unit', 'Beds', 'Baths', 'Beds_Baths', 'SqFt', 'Price', 'PricePerSqFt',
        'RentalType',
        'HasWasherDryer', 'HasAirConditioning', 'HasPool', 'HasSpa',
        'HasGym', 'HasEVCharging', 'StorageFee',
        'AllowsDogs', 'AllowsCats', 'ParkingType',
        'ListingURL'
    ]
    return df[final_order]

def main():
    start = time.time()
    driver = init_driver()
    df = scrape_listings(driver)
    driver.quit()
    if df.empty:
        print("⚠️ No data collected.")
        return
    df = clean_data(df)
    filename = f'san_diego_county_rentals_{datetime.today().strftime("%Y-%m-%d")}.csv'
    df.to_csv(filename, index=False)
    print(f"Saved to: {filename}")
    print(f"⏱️ Runtime: {time.time() - start:.2f} sec")

if __name__ == "__main__":
    main()
