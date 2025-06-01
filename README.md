# BI_Engineer_Task
## Casino Data Analysis Pipeline

## Description
This project implements an end-to-end data processing pipeline for casino performance analytics. It cleanses, transforms, and enriches raw casino transaction data with user demographics, currency conversions, and provider information to produce production-ready business intelligence.

## Key features:

  1. Robust data cleaning and type conversion
  2. Currency conversion to EUR
  3. User Age Group categorization
  4. Gold-level table preparation for analytics

## Files
## Data Files

* casinodaily.csv: Daily casino transaction records
* casinomanufacturers.csv: Manufacturer data
* casinoproviders.csv: Game provider information
* currencyrates.csv: Daily exchange rates
* users.csv: Player demographic data

# Codes & Last Question 
## final.ipynb 
* Uses mostly pandas, numpy, and datetime libraries in order to load, transform and produce the final output.

* ## roadmap.ipynb 
* Is the same as final.ipynd but more analytical in order to present the way of exploring and analyzing new data. It its not made for production levels.

## duckdb.ipynb 
* Uses mostly duckdb and pandas. This is the recommended file to be used in production level.
* DuckDB: can handle datasets larger than my RAM | Pandas: Limited by computer's memory.
* SQL is universal, so its more easier to bee understood
* Also, its easier to move to production databases
* DuckDB processed data in chunks thus, uses minimal memory usage


