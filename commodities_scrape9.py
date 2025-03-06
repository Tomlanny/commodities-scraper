import requests
from bs4 import BeautifulSoup
import logging
import sqlite3
import os
import csv

# Set up logging
logging.basicConfig(level=logging.INFO)

# Database setup
db_path = os.path.join(os.path.expanduser("~"), "OneDrive", "Desktop", "commodities.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS commodities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    name TEXT NOT NULL,
    price TEXT NOT NULL,
    change TEXT NOT NULL,
    volume TEXT NOT NULL,
    low_price TEXT,
    previous_close TEXT,
    expiration_date TEXT,
    production_levels TEXT  -- New column for production levels
)
''')
conn.commit()

def scrape_commodity_data(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.text, 'html.parser')

        commodities = []
        rows = soup.select('tbody tr')  # Select all rows in the table body

        for row in rows:
            ticker = row.select_one('a[data-testid="table-cell-ticker"] .symbol').text.strip()
            name = row.select_one('td:nth-child(2) div').text.strip()
            price = row.select_one('fin-streamer[data-test="change"]').text.strip()
            change = row.select_one('fin-streamer[data-test="colorChange"]').text.strip()
            volume = row.select_one('fin-streamer[data-test="change"][data-field="regularMarketVolume"]').text.strip()
            low_price = row.select_one('fin-streamer[data-field="regularMarketDayLow"]').text.strip() if row.select_one('fin-streamer[data-field="regularMarketDayLow"]') else 'N/A'
            previous_close = row.select_one('fin-streamer[data-field="regularMarketPreviousClose"]').text.strip() if row.select_one('fin-streamer[data-field="regularMarketPreviousClose"]') else 'N/A'
            expiration_date = row.select_one('td:nth-child(3)').text.strip()  # Assuming this is the expiration date

            # Add production levels
            production_levels = scrape_production_levels()  # Call the new function to get production levels

            commodities.append({
                'ticker': ticker,
                'name': name,
                'price': price,
                'change': change,
                'volume': volume,
                'low_price': low_price,
                'previous_close': previous_close,
                'expiration_date': expiration_date,
                'production_levels': production_levels  # Include production levels in the data
            })

        logging.info("Data scraped successfully.")
        return commodities

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return []

def scrape_production_levels():
    url = "https://www.nass.usda.gov/Newsroom"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.text, 'html.parser')

        # Use the provided CSS selector to find the specific div
        news_body = soup.select_one('body > div.container > div.container.mainContent.clearfix > div.contentMain.clearfix > div.contentRight.col-xs-12.col-sm-12.col-md-8.col-lg-8.clearfix > div.clearfix > div.field.field_news_body')
        
        # Extract all paragraphs within that div
        production_data = []
        if news_body:
            for paragraph in news_body.find_all('p'):
                text = paragraph.get_text(strip=True)
                # Append the text to the production_data list
                production_data.append(text)

        # Join all relevant paragraphs into a single string
        return ' '.join(production_data) if production_data else 'N/A'

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching production levels: {e}")
        return 'N/A'

def save_to_database(commodities):
    try:
        for commodity in commodities:
            cursor.execute('''
            INSERT INTO commodities (ticker, name, price, change, volume, low_price, previous_close, expiration_date, production_levels) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (commodity['ticker'], commodity['name'], commodity['price'], commodity['change'], commodity['volume'], commodity['low_price'], commodity['previous_close'], commodity['expiration_date'], commodity['production_levels']))
        conn.commit()
        logging.info("Data saved to database successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")

def export_to_csv(commodities):
    csv_file_path = os.path.join(os.path.expanduser("~"), "OneDrive", "Desktop", "commodities_data.csv")
    try:
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Ticker', 'Name', 'Price', 'Change', 'Volume', 'Low Price', 'Previous Close', 'Expiration Date', 'Production Levels'])  # Header
            for commodity in commodities:
                writer.writerow([commodity['ticker'], commodity['name'], commodity['price'], commodity['change'], commodity['volume'], commodity['low_price'], commodity['previous_close'], commodity['expiration_date'], commodity['production_levels']])
        logging.info(f"Data exported to {csv_file_path}")
    except Exception as e:
        logging.error(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    url = "https://finance.yahoo.com/markets/commodities/?fr=sycsrp_catchall"
    data = scrape_commodity_data(url)
    if data:
        save_to_database(data)
        export_to_csv(data)  # Export data to CSV
        for commodity in data:
            print(commodity)

# Close the database connection
conn.close()