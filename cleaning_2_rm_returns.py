# This file nets out returns from sales data
# uses a Vectorized Merge-As-Of approach for efficiency

import pandas as pd
import numpy as np
import os

# Load The Cleaned Data
data = pd.read_csv(r"DATA\cleaned_data\cleaned_data.csv")

data['date'] = pd.to_datetime(data['date'], errors='coerce')
data['cx_num'] = pd.to_numeric(data['cx_num'], errors='coerce')
data['mat_id'] = pd.to_numeric(data['mat_id'], errors='coerce')
data.dropna(subset=['date'], inplace=True)

# Split Out Sales and Returns
sales = data[data['return_flag'] == 0].copy()
returns = data[data['return_flag'] == 1].copy()
sales.sort_values(["date"], inplace=True, ascending=[True])
sales.reset_index(drop=True, inplace=True)
returns.sort_values(["date"], inplace=True, ascending=[True])
returns.reset_index(drop=True, inplace=True)
sales['sale_date'] = sales['date']
sales['sale_index'] = sales.index

matched = pd.merge_asof(
    returns,
    sales[['date', 'sale_date', 'cx_num', 'mat_id', 'qty', 'val', 'sale_index']],
    by=["cx_num", "mat_id"],
    on="date",
    direction="backward",               
    suffixes=("_return", "_sale"), 
)

indices_2_drop = matched[matched['sale_index'].notna()]['sale_index'].unique().astype(int).tolist()

qty_before = sales['qty'].sum()
print(f"Before Removing Returns:\n   QTY SOLD = {qty_before:,.2f},\n   TOTAL REVENUE = {sales['val'].sum():,.2f}")
sales = sales.drop(index=indices_2_drop)
qty_after = sales['qty'].sum()
print(f"After Removing Returns:\n   QTY SOLD = {qty_after:,.0f},\n   TOTAL REVENUE = {sales['val'].sum():,.2f}")
print(f"% of Qty Removed Due to Returns: {((qty_before - qty_after) / qty_before) * 100:.2f}%")

sales.drop(columns=['sale_date', 'sale_index'], inplace=True)
# Save Netted Sales Data
sales.to_csv(r"DATA\cleaned_data\netted_sales_data.csv", index=False)