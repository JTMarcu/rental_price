from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging

# Set up logging
logging.basicConfig(
    filename='scraper_log.txt', 
    filemode='w', 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)

# Set up Edge WebDriver
edge_options = Options()
edge_options.add_argument("--headless")  # Run in headless mode
edge_options.add_argument("--disable-gpu")
edge_options.add_argument("--no-sandbox")
edge_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

service = Service(EdgeChromiumDriverManager().install())
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
    time.sleep(10)  # You can replace this with WebDriverWait later

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

        all_rentals.append({
            'Title': title.text.strip() if title else None,
            'Address': address.text.strip() if address else None,
            'Price': price.text.strip() if price else None,
            'Beds': beds.text.strip() if beds else None,
            'Phone': phone.get('phone-data') if phone else None,
            'URL': listing.get('data-url')
        })

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
