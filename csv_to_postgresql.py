import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from tkinter import Tk, filedialog

# Load environment variables from .env file
load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# Prompt for CSV files using file dialog
root = Tk()
root.withdraw()  # Hide the main window
file_paths = filedialog.askopenfilenames(
    title="Select one or more rental CSV files to import",
    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
)
root.destroy()

if not file_paths:
    print("No files selected. Exiting.")
    exit()

engine = create_engine(
    f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
)

for csv_file in file_paths:
    print(f'Importing {csv_file} ...')
    df = pd.read_csv(csv_file)
    # Ensure correct types for month/year (best practice)
    df['month'] = df['month'].astype(int)
    df['year'] = df['year'].astype(int)
    df.to_sql(
        'rental_data',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )
    print(f'{os.path.basename(csv_file)} imported successfully!\n')

print('All files loaded into rental_data table in rental_db!')

'''
CREATE TABLE rental_data (
    property_id         VARCHAR(32) NOT NULL,
    property            TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    zipcode             TEXT,
    phone               TEXT,
    unit                TEXT,
    beds                INTEGER,
    baths               NUMERIC,
    beds_baths          TEXT,
    sqft                INTEGER,
    price               NUMERIC,
    pricepersqft        NUMERIC,
    rentaltype          TEXT,
    haswasherdryer      BOOLEAN,
    hasairconditioning  BOOLEAN,
    haspool             BOOLEAN,
    hasspa              BOOLEAN,
    hasgym              BOOLEAN,
    hasevcharging       BOOLEAN,
    ispetfriendly       BOOLEAN,
    listingurl          TEXT,
    month               INTEGER NOT NULL,
    year                INTEGER NOT NULL,
    PRIMARY KEY (property_id, month, year)
);

This is a **PostgreSQL SQL table definition** for a table named `rental_data`. Here's a breakdown of what each part does:

### Table Structure

- **property_id**: A unique identifier for each property (string, required).
- **property, address, city, state, zipcode, phone, unit**: Text fields for property details.
- **beds**: Number of bedrooms (integer).
- **baths**: Number of bathrooms (can be fractional, e.g., 1.5).
- **beds_baths**: Text summary of beds and baths.
- **sqft**: Square footage (integer).
- **price, pricepersqft**: Rental price and price per square foot (numeric, can have decimals).
- **rentaltype**: Type of rental (e.g., apartment, house).
- **haswasherdryer, hasairconditioning, haspool, hasspa, hasgym, hasevcharging, ispetfriendly**: Boolean flags for amenities.
- **listingurl**: URL to the property listing.
- **month, year**: The month and year for the rental data (both required).

### Primary Key

- The **primary key** is a combination of `property_id`, `month`, and `year`.  
  This means each property can have multiple records (e.g., for different months/years), but only one record per property per month/year.

### Gotchas

- **NUMERIC vs INTEGER**: Use `NUMERIC` for values that may have decimals (like price, baths).
- **BOOLEAN**: Stores `TRUE` or `FALSE` values.
- **Composite Primary Key**: Ensures uniqueness for each property per month/year.

### Example Usage

This table is suitable for storing **monthly rental data** for properties, tracking changes over time.

Let me know if you want to see example insert statements or how to query this table!

'''