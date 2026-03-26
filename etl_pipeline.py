"""
Web Scraping & Data Pipeline Project

Description:
This script extracts GDP data of countries from a Wikipedia archive page,
transforms the data, and loads it into a CSV file and SQLite database.

Author: Arjun Baruwal
"""

from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3
from datetime import datetime


# -------------------------------
# Configuration
# -------------------------------
URL = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
CSV_PATH = "Countries_by_GDP.csv"
DB_NAME = "World_Economies.db"
TABLE_NAME = "Countries_by_GDP"
TABLE_COLUMNS = ["Country", "GDP"]


# -------------------------------
# Logging Function
# -------------------------------
def log_progress(message):
    """Logs progress messages with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("log_file.txt", "a") as file:
        file.write(f"{timestamp} - {message}\n")


# -------------------------------
# Extract Function
# -------------------------------
def extract(url, columns):
    """
    Extracts GDP data from the given URL.

    Args:
        url (str): Source URL
        columns (list): Column names

    Returns:
        pd.DataFrame: Extracted data
    """
    df = pd.DataFrame(columns=columns)

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("tbody")
    rows = tables[2].find_all("tr")

    for row in rows:
        cols = row.find_all("td")

        if len(cols) > 0:
            if cols[0].find("a") and "-" not in cols[2].text:
                country = cols[0].a.text.strip()
                gdp = cols[2].text.strip()

                df = pd.concat(
                    [df, pd.DataFrame([[country, gdp]], columns=columns)],
                    ignore_index=True,
                )

    return df


# -------------------------------
# Transform Function
# -------------------------------
def transform(df):
    """
    Cleans and transforms GDP data.

    - Removes commas
    - Converts to float
    - Converts GDP to billions

    Args:
        df (pd.DataFrame): Raw data

    Returns:
        pd.DataFrame: Cleaned data
    """
    df["GDP"] = (
        df["GDP"]
        .replace("—", "0")
        .str.replace(",", "")
        .astype(float)
        / 1000
    )

    df["GDP"] = df["GDP"].round(2)

    return df


# -------------------------------
# Load Functions
# -------------------------------
def load_to_csv(df, path):
    """Saves DataFrame to CSV."""
    df.to_csv(path, index=False)


def load_to_db(df, connection, table_name):
    """Loads DataFrame into SQLite database."""
    df.to_sql(table_name, connection, if_exists="replace", index=False)


# -------------------------------
# Main ETL Pipeline
# -------------------------------
def main():
    log_progress("ETL Job Started")

    # Extract
    log_progress("Extracting data...")
    extracted_data = extract(URL, TABLE_COLUMNS)

    # Transform
    log_progress("Transforming data...")
    transformed_data = transform(extracted_data)

    print("\nTransformed Data:\n", transformed_data.head())

    # Load
    log_progress("Loading data...")
    connection = sqlite3.connect(DB_NAME)

    load_to_csv(transformed_data, CSV_PATH)
    load_to_db(transformed_data, connection, TABLE_NAME)

    # Query Example
    log_progress("Running query...")
    result = pd.read_sql(
        f"SELECT * FROM {TABLE_NAME} WHERE GDP >= 100", connection
    )

    print("\nFiltered Data (GDP >= 100):\n", result)

    result.to_csv("output.csv", index=False)

    # Close connection
    connection.close()

    log_progress("ETL Job Completed Successfully")


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    main()