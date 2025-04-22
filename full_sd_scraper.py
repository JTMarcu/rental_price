from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import logging
import re
from datetime import datetime

# Configuration
HEADLESS = True
LISTINGS_PER_PAGE = 40
WAIT_TIME = 5
MAX_UNITS = 1000
BASE_URL = "https://www.apartments.com/san-diego-ca/"

# Setup Logging
logging.basicConfig(
    filename='scraper_log.txt',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def init_driver():
    options = Options()
    if HEADLESS:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0")
    options.add_argument("--log-level=3")
    service = Service(
        executable_path=EdgeChromiumDriverManager().install(),
        log_output=os.devnull
    )
    return webdriver.Edge(service=service, options=options)

def extract_low_price(price):
    if pd.isna(price):
        return None
    price = re.sub(r'[^\d\-]', '', str(price))
    if '-' in price:
        low = price.split('-')[0].strip()
        return float(low)
    elif price.isdigit():
        return float(price)
    return None

def scrape_listings(driver):
    all_units = []
    page = 1

    while len(all_units) < MAX_UNITS:
        url = f"{BASE_URL}{page}/"
        logging.info(f"Loading page {page}: {url}")
        driver.get(url)
        
        try:
            WebDriverWait(driver, WAIT_TIME).until(
                EC.presence_of_element_located((By.CLASS_NAME, "property-address"))
            )
        except Exception as e:
            logging.warning(f"Timeout on page {page}: {e}")
            break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        listings = soup.find_all('article')
        if not listings:
            break

        for listing in listings:
            if len(all_units) >= MAX_UNITS:
                break

            title = listing.find('span', class_='js-placardTitle')
            address = listing.find('div', class_='property-address')
            phone = listing.find('button', class_='phone-link')
            property_url = listing.get('data-url')

            if title and address and property_url:
                try:
                    driver.get(property_url)
                    WebDriverWait(driver, WAIT_TIME).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "propertyAddress"))
                    )
                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    unit_containers = detail_soup.find_all('li', class_='unitContainer js-unitContainerV3')

                    rental_type = "Unknown"
                    og_title_tag = detail_soup.find("meta", property="og:title")
                    if og_title_tag and og_title_tag.get("content"):
                        content = og_title_tag["content"].lower()
                        if "house rental" in content:
                            rental_type = "House"
                        elif "townhome" in content:
                            rental_type = "Townhome"
                        elif "condo" in content:
                            rental_type = "Condo"
                        elif "apartment" in content:
                            rental_type = "Apartment"

                    for unit in unit_containers:
                        if len(all_units) >= MAX_UNITS:
                            break
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
                            'ListingURL': property_url
                        })

                        if len(all_units) % 100 == 0:
                            print(f"📦 {len(all_units)} units scraped...")

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
    return df

def main():
    try:
        driver = init_driver()
        df = scrape_listings(driver)
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print("A critical error occurred. Check logs.")
        return
    finally:
        driver.quit()

    df = clean_data(df)
    today_str = datetime.today().strftime('%Y-%m-%d')
    filename = f'all_sd_rentals_{today_str}.csv'

    if df.empty:
        print("⚠️ No data collected. File not saved.")
        logging.warning("No data collected. File not saved.")
        return

    df.to_csv(filename, index=False)
    print(f"Scraping complete. {len(df)} rows saved to {filename}")
    logging.info(f"Scraping complete. Data saved to {filename}")

if __name__ == "__main__":
    main()