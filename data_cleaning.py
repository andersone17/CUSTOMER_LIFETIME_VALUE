# File to Clean and Stack ECC Data

# Notes:
    # Orders with a cancellation reason were not completed and should be excluded from analysis.
    # ZREU: Returns. These should be deduped (subset="ORder Number", "Customer", "Material")
    # After deduping returns, we should mutlply qty and val by -1 to reflect the return.
    # We can then group by order number, customer, material to get net qty and val.

import pandas as pd
import numpy as np
import os


###############################################################################
# BYD Data Cleaning
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
data['ZIP'] = pd.to_numeric(data['ZIP'], errors='coerce')
data = data[['DATE', 'SO', 'Ship-To', 'CX_NUM', 'DESC1',
             'ADDRESS','ZIP','STATE','PID1','KMAT','OLDMATNO', 
             'VAL','QTY']]
data.columns = [col.lower() for col in data.columns]
data['return_flag'] = np.where(data['qty'] < 0, 1, 0)
data['erp'] = 'BYD'
print(f"Loaded BYD data with {data.shape[0]:,} records")


###############################################################################
# ECC DATA CLEANING
files = os.listdir(r"DATA\ECC_SOV")
column_names = {
    "SD Document" : 'so',
    "Distribution channel" : 'dist_channel',
    "Created On" : 'date',
    "Created By" : 'created_by',
    "Sales Document Type" : 'sales_doc_type',
    "Sales order type" : 'sales_order_type',
    "Customer" : 'customer',
    "Name" : 'name',
    "Name 2" : 'name_2',
    "Street" : 'street',
    "Postal Code" : 'zip',
    "City" : 'city',
    "Country ship-to" : 'country',
    "Complete delivery" : 'complete_delivery',
    "Pur. Order" : 'pur_order',
    "Status total" : 'status_total',
    "Reason for rejection" : 'reason_for_rejection',
    "Cancellation reason" : 'cancellation_reason',
    "Material" : 'material',
    "Short txt customer order item" : 'desc',
    "Plant" : 'plant',
    "Storage Location" : 'storage_location',
    "Delivery Date" : 'delivery_date',
    "Material Type" : 'material_type',
    "Material Group" : 'material_group',
    "Material Group Desc." : 'material_group_desc',
    "Net value" : 'net_value',
    "Tax amount" : 'tax_amount',
    "Confirmed Quantity" : 'confirmed_quantity',
    "Cancellation Date" : 'cancellation_date',
    "Region" : 'region',
    "Batch" : 'batch'
}
frames = []
for file in files:
    df = pd.read_excel(f"DATA\\ECC_SOV\\{file}")
    df.rename(columns=column_names, inplace=True)
    df = df[list(column_names.values())]
    frames.append(df)
    print(f"Loaded {file} with {df.shape[0]:,} records")
ecc_data = pd.concat(frames, ignore_index=True)
ecc_data = ecc_data[['date', 'so', 'dist_channel', 'sales_doc_type', 'customer',
                     'name', 'name_2', 'street', 'zip', 'city', 'country',
                     'reason_for_rejection', 'cancellation_reason', 'material',
                     'desc', 'material_type', 'net_value', 'tax_amount', 'confirmed_quantity', 
                     'batch']]
ecc_data['date'] = pd.to_datetime(ecc_data['date'], errors='coerce')
num_cols = ['so', 'customer',  'reason_for_rejection', 'material', 'net_value', 
            'tax_amount', 'confirmed_quantity', 'batch']
for col in num_cols:
    ecc_data[col] = ecc_data[col].astype(str).str.replace(',', '', regex=False).str.strip()
    ecc_data[col] = pd.to_numeric(ecc_data[col], errors='coerce')
text_cols = ['dist_channel', 'sales_doc_type', 'name', 'name_2', 
             'street', 'city', 'country', 'cancellation_reason',
             'desc', 'material_type']
for col in text_cols:
    ecc_data[col] = ecc_data[col].astype(str).str.strip().str.upper()
ecc_data['zip'] = ecc_data['zip'].astype(str).str.strip().str[:5] # ZIP codes: keep only first 5 digits
ecc_data['zip'] = pd.to_numeric(ecc_data['zip'], errors='coerce')
ecc_data['return_flag'] = np.where(ecc_data['sales_doc_type'] == 'ZREU', 1, 0)
ecc_data.drop_duplicates(subset = ['so', 'return_flag', 'customer', 'material'], inplace=True) # Dedup

# Clean Return Data
returns = ecc_data[ecc_data['return_flag'] == 1].copy()
returns = returns[returns['reason_for_rejection'].isna()] # Exclude Canceled Returns (Stan Confirmed this)
returns.drop_duplicates(subset=['customer', 'material'], inplace=True) # Customers can create multiple return orders for same material...

# Sales Data
sales = ecc_data[ecc_data['return_flag'] == 0].copy()
sales = sales[sales['reason_for_rejection'].isna()] # Exclude Canceled Sales Orders

ecc_data = pd.concat([sales, returns], ignore_index=True)


############################################################################################################################
# Get BYD Data and ECC Data into Common Format to Concatenate
    # BYD is more restricted, so we will match BYD format


# Save File







############################################################################################################################
# This can be done in a seperate file after concatenation
# That way we only have to do this process once, not every time we run. 
# When we update data, we will just stack it onto the cleaned set of data and deuplicate again.
# Filtering Out Returns by Matching Returns to the Most Recent Sales (This should be done at the end)
sales = data[data["qty"] > 0].copy()
returns = data[data["qty"] < 0].copy()

def match_sales_and_returns(sales_df, returns_df):
    '''We match the most recent sale for each return'''
    sales_df.sort_values(["cx_num", "pid1", "date"], inplace=True, ascending=[True, True, True])
    returns_df.sort_values(["cx_num", "pid1", "date"], inplace=True, ascending=[True, True, False])
    
    returned_rows = []
    used_rows = set()
    for i, r in returns_df.iterrows():
        return_qty = r['qty']
        prior_sales = sales_df[(sales_df['date'] < r['date']) &
                               (sales_df['cx_num'] == r['cx_num']) &
                               (sales_df['pid1'] == r['pid1']) &
                               (~sales_df.index.isin(used_rows))
                               ]
        if len(prior_sales) > 0:
            last_idx = prior_sales.index[-1]
            returned_rows.append(last_idx)  # most recent sale
            used_rows.add(last_idx)
        else:
            continue
    return returned_rows


returned_indices = match_sales_and_returns(sales, returns)

sales['returned'] = sales.index.isin(returned_indices)
kept_sales = sales[~sales['returned']].copy()
