"""
1. Import necessay libraries.
2. Load MySQL and Postgres connection details
3. Build connection strings and build database engines
4. Read product table from MySQL and load into dataframe
5. Write dataframe to products table in Postgres (raw schema)
"""
#%%
# Import necessary libraries
import pandas as pd 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
#%%
# Load environment variables for dotenv file
load_dotenv()
#%%
os.environ['MYSQL_USER']
#%%
# MySQL database connection details
mysql_user = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
mysql_host = os.environ['MYSQL_HOST']
mysql_db = os.environ['MYSQL_DB']

# Postgres database connection details
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']
# %%
# Build Connection Strings
mysql_conn_str = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}'
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'
# %%
#Create Database Engines
mysql_engine = create_engine(mysql_conn_str)
pg_engine = create_engine(pg_conn_str)
# %%
# Read orders table from mysql
df = pd.read_sql('SELECT * FROM orders', mysql_engine)
# %%
#df
# %%
# Write DF to orders table in Postgres (raw schema)
df.to_sql('orders', pg_engine, schema='raw', if_exists='replace', index=False)
# %%
print(f'{len(df)}records loaded into orders table')
# %%
