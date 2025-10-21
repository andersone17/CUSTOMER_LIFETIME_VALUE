# File to Clean and Stack ECC Data

# Notes:
    # Orders with a cancellation reason were not completed and should be excluded from analysis.
    # ZREU: Returns. These should be deduped (subset="ORder Number", "Customer", "Material")
    # After deduping returns, we should mutlply qty and val by -1 to reflect the return.
    # We can then group by order number, customer, material to get net qty and val.

import pandas as pd
import numpy as np
import os


# First Clean BYD Data
data = pd.read_excel("DATA/BYD_Invoiced_18_23.xlsx")
data['DATE'] = pd.to_datetime(data['DATE'], errors='coerce')
numeric_cols = ['SO', 'ITEM', 'CX_NUM', 'PID1', 'KMAT', 'OLDMATNO', 
                'KMAT2', 'PID2', 'VAL', 'QTY']
for col in numeric_cols:
    data[col] = data[col].astype(str).str.replace(',', '', regex=False).str.strip()
    data[col] = pd.to_numeric(data[col], errors='coerce')
text_cols = ['DESC1', 'Ship-To', 'ADDRESS', 'ZIP', 'STATE', 
             'DESC2', 'DRIVE', 'WORLD']
for col in text_cols:
    data[col] = data[col].astype(str).str.strip().str.upper()
# ZIP codes: keep only first 5 digits
data['ZIP'] = data['ZIP'].astype(str).str.strip().str[:5]
data = data[['DATE', 'SO', 'Ship-To', 'CX_NUM',
             'ADDRESS','ZIP','STATE','PID1','KMAT','OLDMATNO', 
             'VAL','QTY']]
data.columns = [col.lower() for col in data.columns]

# Need to figure out how to  group the returns...
    # We want to net out returns, but also need to take into account that 
    # someone could buy the same product multiple times...
    # We could get the returns, and merge back onto the orginal data, 
        # with only one return per order/customer/material combination.



# Group by Customer and Material to net out returns
grouped = data.groupby(['cx_num', 'pid1']).agg({
    'date' : 'max', 
    'so' : 'last',
    'ship-to' : 'first',
    'address' : 'first',
    'zip' : 'first',
    'state' : 'first',
    'kmat' : 'first',
    'oldmatno' : 'first',
    'val' : 'sum',
    'qty' : 'sum'
}).reset_index()




data.columns.tolist()

# ECC Data
files = os.listdir(r"DATA\SOV\ZORDERLIST")
column_names = []

frames = []
df = pd.read_excel(f"DATA\\SOV\\ZORDERLIST\\{files[0]}")


