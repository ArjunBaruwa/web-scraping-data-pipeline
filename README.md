# Web Scraping & Data Pipeline Project

## Overview
This project extracts GDP data of countries from a Wikipedia archive page, transforms it, and loads it into a CSV file and SQLite database. It demonstrates a full ETL pipeline with logging, data cleaning, and storage.

## Tech Stack
Python | BeautifulSoup | Pandas | SQLite | CSV

## Features
- Extracts GDP data from web pages using web scraping
- Cleans and transforms raw data (removes commas, converts to billions)
- Loads data into CSV and SQLite database
- Logs progress and errors
- Queries database for filtered data

## How to Run
1. Install dependencies: `pip install -r requirements.txt`
2. Run the script: `python etl_pipeline.py`
3. Output CSV and database will be created in the same folder

## Outcome
This project demonstrates a real-world ETL workflow from web data extraction to storage, including transformation and logging.