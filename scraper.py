
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import logging
import re
from datetime import datetime

HEADLESS = True
WAIT_TIME = 4
LISTINGS_PER_PAGE = 40
BASE_URL = "https://www.apartments.com/apartments-condos/san-diego-county-ca/under-4000/"
LOG_FILE = "scraper_log.txt"
TEST_MODE = False
MAX_UNITS = 10

logging.basicConfig(
    filename=LOG_FILE,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.WARNING
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

def extract_low_price(price):
    if pd.isna(price):
        return None
    price = re.sub(r'[^\d\-]', '', str(price))
    return float(price.split('-')[0]) if '-' in price else float(price) if price.isdigit() else None

def extract_amenities(soup):
    labels = soup.select('.amenityLabel') + soup.select('.combinedAmenitiesList li span')
    fee_section = soup.find(id='fees-policies-pets-tab')
    
    # Combine visible amenities and pet policy tab content
    text = ' '.join(el.get_text(separator=' ').lower().strip() for el in labels)
    if fee_section:
        text += ' ' + fee_section.get_text(separator=' ').lower()

    logging.debug("Combined amenities and fee policy text: %s", text)

    return {
        'HasWasherDryer': 'washer/dryer' in text or 'in unit washer' in text,
        'HasAirConditioning': 'air conditioning' in text,
        'HasPool': 'pool' in text,
        'HasSpa': 'spa' in text or 'hot tub' in text,
        'HasGym': 'fitness center' in text or 'gym' in text,
        'HasEVCharging': 'ev charging' in text,
        'IsPetFriendly': any(pet in text for pet in ['dog friendly', 'dogs allowed', 'cat friendly', 'cats allowed'])
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
                            'Property': title.text.strip(),
                            'Address': address.text.strip(),
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

def clean_data(df):
    df['Price'] = df['Price'].apply(extract_low_price)
    df['SqFt'] = pd.to_numeric(df['SqFt'].astype(str).str.replace(',', '').str.extract(r'(\d+)', expand=False), errors='coerce')
    df['Beds'] = pd.to_numeric(df['Beds'], errors='coerce')
    df['Baths'] = pd.to_numeric(df['Baths'], errors='coerce')

    df['ZipCode'] = df['Address'].str.extract(r'(\d{5})(?!.*\d{5})')
    city_state = df['Address'].str.extract(r',\s*([^,]+),\s*([A-Z]{2})\s*\d{5}')
    df['City'] = city_state[0].str.strip()
    df['State'] = city_state[1].str.strip()

    df['PricePerSqFt'] = df.apply(
        lambda row: round(row['Price'] / row['SqFt'], 2)
        if pd.notnull(row['Price']) and pd.notnull(row['SqFt']) and row['SqFt'] > 0 else None,
        axis=1
    )

    df['Beds_Baths'] = df.apply(
        lambda row: f"{int(row['Beds']) if pd.notnull(row['Beds']) else 'N/A'} Bed / {int(row['Baths']) if pd.notnull(row['Baths']) else 'N/A'} Bath",
        axis=1
    )

    final_order = [
        'Property', 'Address', 'City', 'State', 'ZipCode', 'Phone',
        'Unit', 'Beds', 'Baths', 'Beds_Baths', 'SqFt', 'Price', 'PricePerSqFt',
        'RentalType',
        'HasWasherDryer', 'HasAirConditioning', 'HasPool', 'HasSpa',
        'HasGym', 'HasEVCharging',
        'IsPetFriendly',
        'ListingURL'
    ]
    return df[final_order]

def main():
    start_time = time.time()
    driver = init_driver()
    df = scrape_listings(driver)
    driver.quit()

    if df.empty:
        print("No data collected. File not saved.")
        logging.warning("No data collected. File not saved.")
    else:
        df = clean_data(df)
        filename = f'san_diego_county_rentals_{datetime.today().strftime("%Y-%m-%d")}.csv'
        df.to_csv(filename, index=False)
        print(f"Scraping complete. Data saved to {filename}")
        logging.info(f"Scraping complete. Data saved to {filename}")

    duration = time.time() - start_time
    minutes, seconds = divmod(duration, 60)
    print(f"Script runtime: {int(minutes)} minutes and {seconds:.2f} seconds")

if __name__ == "__main__":
    main()
