# Script to Clean the BYD Sales Order Volume Data
# This only needs to be done once, because BYD data ends in 2023 and is static.

import pandas as pd
import numpy as np
import os


columns_names = [
    'date', 
    'sales_order',
    'kmat', 
    'description',
    'zfer',
    'old_mat',
    'name', 
    'cust_id',
    'phone',
    'email',
    'address',
    'house_no',
    'street',
    'city',
    'state',
    'postal_code',
    'req_qty',
    'invoice_qty',
    'net_value'    
]

frames = []
for file in os.listdir(r"DATA\SOV\BYD_EXPORTS"):
    df = pd.read_csv(f"DATA\\SOV\\BYD_EXPORTS\\{file}")
    df.columns = columns_names
    frames.append(df)
byd = pd.concat(frames)

byd['date'] = pd.to_datetime(byd['date'], format='%m/%d/%Y', errors='coerce')
numeric_cols = ['sales_order', 'kmat', 'zfer', 'old_mat', 'cust_id', 'req_qty', 'invoice_qty', 'net_value']
for col in numeric_cols:
    byd[col] = byd[col].astype(str).str.replace(r'[^0-9.]', '', regex=True).str.strip()
    byd[col] = pd.to_numeric(byd[col], errors='coerce')
text_cols = ['description', 'name', 'email', 'address', 'house_no', 'street', 'city', 'state']
for col in text_cols:
    byd[col] = byd[col].astype(str).str.strip().str.upper()
# ZIP codes: keep only first 5 digits
byd['postal_code'] = byd['postal_code'].astype(str).str.strip().str[:5]
byd['postal_code'] = pd.to_numeric(byd['postal_code'], errors='coerce')
# Phone Numer: Keep only the last 10 digits
byd['phone'] = byd['phone'].astype(str).str.replace(r'[^0-9]', '', regex=True).str.strip()
byd['phone'] = byd['phone'].str[-10:]
byd['phone'] = pd.to_numeric(byd['phone'], errors='coerce').astype('Int64')

# Save Cleaned Data
byd.to_csv("DATA//cleaned_data//byd_sov.csv", index=False)