from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import logging
import subprocess

# Set up logging
logging.basicConfig(
    filename='scraper_log.txt', 
    filemode='w', 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)

# Set up Edge WebDriver in headless mode with fully suppressed output
edge_options = Options()
edge_options.add_argument("--headless")
edge_options.add_argument("--disable-gpu")
edge_options.add_argument("--no-sandbox")
edge_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

service = Service(
    executable_path=EdgeChromiumDriverManager().install(),
    log_output=subprocess.DEVNULL
)

driver = webdriver.Edge(service=service, options=edge_options)

# Begin scraping all pages
all_units = []
page = 1
listings_per_page = 40

while True:
    url = f'https://www.apartments.com/apartments-condos/san-diego-ca/min-2-bedrooms-2-bathrooms-2000-to-3500/{page}/'
    log_msg = f"Loading page {page}: {url}"
    print(f"\n{log_msg}")
    logging.info(log_msg)

    driver.get(url)
    time.sleep(10)

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    listings = soup.find_all('article')

    found_msg = f"Found {len(listings)} listings on page {page}"
    print(found_msg)
    logging.info(found_msg)

    if len(listings) == 0:
        logging.info("No listings found — breaking loop.")
        break

    for listing in listings:
        title = listing.find('span', class_='js-placardTitle')
        address = listing.find('div', class_='property-address')
        phone = listing.find('button', class_='phone-link')
        property_url = listing.get('data-url')

        if title and address and property_url:
            try:
                driver.get(property_url)
                time.sleep(5)
                detail_html = driver.page_source
                detail_soup = BeautifulSoup(detail_html, 'html.parser')
                unit_containers = detail_soup.find_all('li', class_='unitContainer js-unitContainerV3')

                if not unit_containers:
                    logging.warning(f"No unit data found for {property_url}")

                for unit in unit_containers:
                    unit_number = unit.find('div', class_='unitColumn column')
                    price = unit.find('div', class_='pricingColumn column')
                    sqft = unit.find('div', class_='sqftColumn column')

                    all_units.append({
                        'Property': title.text.strip(),
                        'Address': address.text.strip(),
                        'Unit': unit_number.text.strip() if unit_number else "N/A",
                        'Price': price.text.strip() if price else "N/A",
                        'SqFt': sqft.text.strip() if sqft else "N/A",
                        'Phone': phone.get('phone-data') if phone and phone.has_attr('phone-data') else "N/A",
                        'ListingURL': property_url
                    })
            except Exception as e:
                logging.warning(f"Error loading unit data for {property_url}: {e}")

        else:
            logging.warning("Skipped listing with missing title, address, or URL.")

    if len(listings) < listings_per_page:
        logging.info("Reached last page.")
        break

    page += 1

# Save to CSV
df = pd.DataFrame(all_units)
df.to_csv('san_diego_unit_level_rentals.csv', index=False)
completion_msg = "Scraping complete. Data saved to san_diego_unit_level_rentals.csv"
print(completion_msg)
logging.info(completion_msg)

# Close the browser
driver.quit()
logging.info("Browser session closed.")
