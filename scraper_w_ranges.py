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
    log_output=subprocess.DEVNULL  # Suppress EdgeDriver output completely
)

driver = webdriver.Edge(service=service, options=edge_options)

# Begin scraping all pages
all_rentals = []
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
        price = listing.find('p', class_='property-pricing')
        beds = listing.find('p', class_='property-beds')
        phone = listing.find('button', class_='phone-link')
        url = listing.get('data-url')

        # Only include listings with title and address
        if title and address:
            all_rentals.append({
                'Title': title.text.strip(),
                'Address': address.text.strip(),
                'Price': price.text.strip() if price else "N/A",
                'Beds': beds.text.strip() if beds else "N/A",
                'Phone': phone.get('phone-data') if phone and phone.has_attr('phone-data') else "N/A",
                'URL': url if url else "N/A"
            })
        else:
            logging.warning("Skipped listing with missing title or address.")

    if len(listings) < listings_per_page:
        logging.info("Reached last page.")
        break

    page += 1

# Save to CSV
df = pd.DataFrame(all_rentals)
df.to_csv('san_diego_rentals_auto_paginated.csv', index=False)
completion_msg = "Scraping complete. Data saved to san_diego_rentals_auto_paginated.csv"
print(completion_msg)
logging.info(completion_msg)

# Close the browser
driver.quit()
logging.info("Browser session closed.")
