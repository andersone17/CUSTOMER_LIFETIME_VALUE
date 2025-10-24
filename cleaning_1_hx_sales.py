# File to Clean and Stack ECC Data

# Notes:
    # Orders with a cancellation reason were not completed and should be excluded from analysis.
    # ZREU: Returns. These should be deduped (subset="ORder Number", "Customer", "Material")
    # After deduping returns, we should mutlply qty and val by -1 to reflect the return.
    # We can then group by order number, customer, material to get net qty and val.

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta



###############################################################################
# CHECK FOR THE CLEAN FILE TO SEE WHAT NEEDS TO BE UPDATED
if os.path.exists(r"DATA\cleaned_data\cleaned_data.csv"):
    clean_data = pd.read_csv(r"DATA\cleaned_data\cleaned_data.csv")
    last_date = clean_data['date'].max()
    last_date = datetime.strptime(last_date, '%Y-%m-%d')
    clean_data['date'] = pd.to_datetime(clean_data['date'], errors='coerce')
    num_cols = ['so_num', 'cx_num', 'zip', 'state', 'mat_id', 'kmat', 
                'val', 'qty', 'return_flag']
    for col in num_cols:
        clean_data[col] = clean_data[col].astype(str).str.replace(',', '', regex=False).str.strip()
        clean_data[col] = pd.to_numeric(clean_data[col], errors='coerce')
    print('Cleaned Data Loaded Ending on', last_date.strftime('%Y-%m-%d'))
else:
    clean_data = pd.DataFrame()
    last_date = datetime(2000, 1, 1)  # Arbitrary old date to ensure all data is processed



###############################################################################
# BYD Data Cleaning
if last_date <= datetime(2023, 12, 31):
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
    data.drop(columns=['address', 'oldmatno'], inplace=True)
    data.rename(columns={
        'ship-to': 'cx_name',
        'so': 'so_num',
        'desc1' : 'desc',
        'pid1' : 'mat_id'}, inplace=True)
    col_order = ['date', 'so_num', 'cx_num', 'cx_name', 'zip', 'state', 'mat_id',
                'kmat', 'desc', 'val', 'qty', 'return_flag', 'erp']
    data = data[col_order]
    print(f"Loaded BYD data with {data.shape[0]:,} records")
else:
    data = None

###############################################################################
# ECC DATA CLEANING
files = os.listdir(r"DATA\ECC_SOV")
new_files = []
for new_ecc_file in files:
    start_date = new_ecc_file.split("_")[-2]
    start_date = datetime.strptime(start_date, '%Y%m%d')
    if start_date > last_date:
        new_files.append(new_ecc_file)
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
    "Region" : 'state',
    "Batch" : 'batch'
}
frames = []
for file in new_files:
    df = pd.read_excel(f"DATA\\ECC_SOV\\{file}")
    df.rename(columns=column_names, inplace=True)
    df = df[list(column_names.values())]
    frames.append(df)
    print(f"Loaded {file} with {df.shape[0]:,} records")
if frames:
    ecc_data = pd.concat(frames, ignore_index=True)
    ecc_data = ecc_data[['date', 'so', 'dist_channel', 'sales_doc_type', 'customer',
                        'name', 'name_2', 'street', 'zip', 'city', 'state', 'country',
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
                'desc', 'material_type', 'state']
    for col in text_cols:
        ecc_data[col] = ecc_data[col].astype(str).str.strip().str.upper()
    ecc_data['zip'] = ecc_data['zip'].astype(str).str.strip().str[:5] # ZIP codes: keep only first 5 digits
    ecc_data['zip'] = pd.to_numeric(ecc_data['zip'], errors='coerce')
    ecc_data['return_flag'] = np.where(ecc_data['sales_doc_type'] == 'ZREU', 1, 0)
    ecc_data.drop_duplicates(subset = ['so', 'return_flag', 'customer', 'material'], 
                            inplace=True) # Dedup
    # Clean Return Data
    returns = ecc_data[ecc_data['return_flag'] == 1].copy()
    returns = returns[returns['reason_for_rejection'].isna()] # Exclude Canceled Returns (Stan Confirmed this)
    returns.drop_duplicates(subset=['customer', 'material'], inplace=True) # Customers can create multiple return orders for same material...
    returns['confirmed_quantity'] = -1 * returns['confirmed_quantity']
    returns['net_value'] = -1 * returns['net_value']

    # Sales Data
    sales = ecc_data[ecc_data['return_flag'] == 0].copy()
    sales = sales[sales['reason_for_rejection'].isna()] # Exclude Canceled Sales Orders

    ecc_data = pd.concat([sales, returns], ignore_index=True)
    ecc_data['erp'] = 'ECC'
    print(f"Processed ECC data with {ecc_data.shape[0]:,} records after cleaning")

    # FILTER OUT DIST CHANNEL != WEBSHOP?????
    # Prepare for Concatenation with BYD Data
    ecc_data.rename(columns={
        'so' : 'so_num',
        'customer' : 'cx_num',
        'name' : 'cx_name',
        'material' : 'mat_id',
        'net_value' : 'val',
        'confirmed_quantity' : 'qty'}, inplace=True)
    ecc_data.drop(columns=['dist_channel', 'sales_doc_type', 'name_2', 'city',
                        'country', 'reason_for_rejection', 'cancellation_reason', 
                        'material_type', 'batch', 'tax_amount', 'street'], inplace=True)
    ecc_data['kmat'] = np.nan
    col_order = ['date', 'so_num', 'cx_num', 'cx_name', 'zip', 'state', 'mat_id',
                'kmat', 'desc', 'val', 'qty', 'return_flag', 'erp']
    ecc_data = ecc_data[col_order]
else:
    ecc_data = None




############################################################################################################################
# CONCAT
if data and ecc_data:
    final_data = pd.concat([data, ecc_data], ignore_index=True)
elif ecc_data:
    final_data = pd.concat([clean_data, ecc_data], ignore_index=True)
else: 
    final_data = None
    print('No files to concatenate. Exiting.')

# Drop Duplicates of Date, SO, CX_NUM, MAT_ID, RETURN_FLAG
if final_data:
    final_data.drop_duplicates(subset=['date', 'so_num', 'cx_num', 'mat_id',
                                       'return_flag'], 
                               inplace=True)
    final_data.to_csv(r"DATA\cleaned_data\cleaned_data.csv", index=False)
    print(f"Exported/Updated Cleaned Data")
