# Import the necessary libraries
import duckdb
import pandas as pd
import numpy as np
from datetime import date

# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

# Start DuckDB connection
conn = duckdb.connect(':memory:')  # In-memory database

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Define Paths
file_paths = {'casinodaily': r"\Users\dimit\OneDrive\Desktop\Novibet\casinodaily_BI_Engineer\casinodaily.csv",
    'casinomanufacturers': r"\Users\dimit\OneDrive\Desktop\Novibet\casinodaily_BI_Engineer\casinomanufacturers.csv",
    'casinoproviders': r"\Users\dimit\OneDrive\Desktop\Novibet\casinodaily_BI_Engineer\casinoproviders.csv",
    'currencyrates': r"\Users\dimit\OneDrive\Desktop\Novibet\casinodaily_BI_Engineer\currencyrates.csv",
    'users': r"\Users\dimit\OneDrive\Desktop\Novibet\casinodaily_BI_Engineer\users.csv"}

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Load CSVs into DuckDB tables
for table_name, file_path in file_paths.items():
    if table_name != 'casinomanufacturers':  # handle this one separately
        conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{file_path}')
        """)

# Special handling for casinomanufacturers (problematic csv)
df_manufacturers = pd.read_csv(
    file_paths['casinomanufacturers'],
    skiprows=1,
    header=None,
    names=["CasinoManufacturerId", "CasinoManufacturerName", "FromDate", "ToDate", "LatestFlag"])

# Clean the manufacturers data
df_manufacturers = df_manufacturers.replace('"', '', regex=True)
null_mask = df_manufacturers['LatestFlag'].isnull()

for idx in df_manufacturers[null_mask].index:
    checkdata = str(df_manufacturers.loc[idx, 'CasinoManufacturerId'])
    parts = checkdata.split(',')
    if len(parts) >= 5:
        df_manufacturers.loc[idx, 'CasinoManufacturerId'] = parts[0].strip()
        df_manufacturers.loc[idx, 'CasinoManufacturerName'] = parts[1].strip()
        df_manufacturers.loc[idx, 'FromDate'] = parts[2].strip()
        df_manufacturers.loc[idx, 'ToDate'] = parts[3].strip() if parts[3].strip() else None
        df_manufacturers.loc[idx, 'LatestFlag'] = parts[4].strip()

# Load cleaned manufacturers data into DuckDB
conn.register('casinomanufacturers_temp', df_manufacturers)
conn.execute("""
    CREATE TABLE casinomanufacturers AS 
    SELECT * FROM casinomanufacturers_temp
""")

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Data cleaning and transformations using SQL

# Remove duplicates from all tables
tables_to_check = ['casinodaily', 'casinomanufacturers', 'casinoproviders', 'currencyrates', 'users']
for table in tables_to_check:
    conn.execute(f"""
        CREATE TABLE {table}_clean AS
        SELECT DISTINCT * FROM {table}
    """)
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {table}_clean RENAME TO {table}")

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Create cleaned and typed tables

# Clean casinodaily

# check the data types
date_type_check = conn.execute("DESCRIBE casinodaily").df()
print("CasinoDaily table structure:")
print(date_type_check)

# Create processed table with appropriate date handling
conn.execute("""
    CREATE TABLE casinodaily_processed AS
    SELECT 
        Date,  -- Already date type, no conversion needed
        UserID,
        CasinoManufacturerId,
        CasinoProviderId,
        CurrencyId,
        GGR,
        Returns
    FROM casinodaily
""")

# Clean casinomanufacturers with proper types and latest records only
manufacturers_type_check = conn.execute("DESCRIBE casinomanufacturers").df()
print("CasinoManufacturers table structure:")
print(manufacturers_type_check)

conn.execute("""
    CREATE TABLE casinomanufacturers_processed AS
    SELECT 
        CAST(CasinoManufacturerId AS INTEGER) as CasinoManufacturerId,
        CasinoManufacturerName,
        TRY_CAST(FromDate AS DATE) as FromDate,
        TRY_CAST(ToDate AS DATE) as ToDate,
        CAST(LatestFlag AS INTEGER) as LatestFlag
    FROM casinomanufacturers
    WHERE TRY_CAST(LatestFlag AS INTEGER) = 1
""")

# Clean currencyrates with proper date conversion
currency_type_check = conn.execute("DESCRIBE currencyrates").df()
print("CurrencyRates table structure:")
print(currency_type_check)

conn.execute("""
    CREATE TABLE currencyrates_processed AS
    SELECT 
        Date,  -- Already DATE type from auto-detection
        FromCurrencyId,
        ToCurrencyId,
        ToCurrencySysname,
        EuroRate
    FROM currencyrates
""")

# Set user Age and Age Groups
current_date = date.today().strftime('%Y-%m-%d')

# Check users table structure
users_type_check = conn.execute("DESCRIBE users").df()
print("Users table structure:")
print(users_type_check)

conn.execute(f"""
    CREATE TABLE users_processed AS
    SELECT 
        user_id,
        Country,
        Sex,
        BirthDate,  -- Already DATE type, no conversion needed
        VIPStatus,
        -- Calculate age using DATE arithmetic
        CASE 
            WHEN BirthDate IS NOT NULL THEN
                CAST((DATE '{current_date}' - BirthDate) / 365 AS INTEGER)
            ELSE NULL
        END as Age
    FROM users
    WHERE BirthDate IS NOT NULL
""")

# Add Age Groups
conn.execute("""
    CREATE TABLE users_final AS
    SELECT *,
        CASE 
            WHEN Age < 18 THEN 'Under 18'
            WHEN Age BETWEEN 21 AND 26 THEN '21-26'
            WHEN Age BETWEEN 27 AND 32 THEN '27-32'
            WHEN Age BETWEEN 33 AND 40 THEN '33-40'
            WHEN Age BETWEEN 41 AND 50 THEN '41-50'
            WHEN Age > 50 THEN '50+'
            ELSE 'Other'
        END as AgeGroup
    FROM users_processed
""")

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Replace the users_processed table
conn.execute("DROP TABLE users_processed")
conn.execute("ALTER TABLE users_final RENAME TO users_processed")

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Create the main fact table with currency conversion
conn.execute("""
    CREATE TABLE final_dataset AS
    WITH currency_converted AS (
        SELECT 
            cd.Date,
            cd.UserID,
            cd.CasinoManufacturerId,
            cd.CasinoProviderId,
            cd.CurrencyId,
            -- Apply currency conversion
            CASE 
                WHEN cr.EuroRate IS NOT NULL THEN cd.GGR * cr.EuroRate
                ELSE cd.GGR
            END as GGR,
            CASE 
                WHEN cr.EuroRate IS NOT NULL THEN cd.Returns * cr.EuroRate
                ELSE cd.Returns
            END as Returns
        FROM casinodaily_processed cd
        LEFT JOIN currencyrates_processed cr 
            ON cd.Date = cr.Date AND cd.CurrencyId = cr.ToCurrencyId
    )
    SELECT 
        cc.Date,
        u.Country,
        u.Sex,
        u.AgeGroup,
        u.VIPStatus,
        cm.CasinoManufacturerName,
        cp.CasinoProviderName,
        cc.GGR,
        cc.Returns
    FROM currency_converted cc
    LEFT JOIN users_processed u ON cc.UserID = u.user_id
    LEFT JOIN casinomanufacturers_processed cm ON cc.CasinoManufacturerId = cm.CasinoManufacturerId
    LEFT JOIN casinoproviders cp ON cc.CasinoProviderId = cp.CasinoProviderId
""")

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# Get the final result as a pandas df
final_df = conn.execute("SELECT * FROM final_dataset").df()
print(final_df.head())