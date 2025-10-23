# This file nets out returns from sales data

import pandas as pd
import numpy as np
import os


# Load The Cleaned Data
data = pd.read_csv(r"DATA\cleaned_data\cleaned_data.csv")

# Split Out Sales and Returns
sales = data[data['return_flag'] == 0].copy()
returns = data[data['return_flag'] == 1].copy()

# Sort dataframes reverse to match most recent sales first, and for faster iteration
sales.sort_values(["cx_num", "mat_id", "date"], inplace=True, ascending=[True, True, False])
returns.sort_values(["cx_num", "mat_id", "date"], inplace=True, ascending=[True, True, False])

def match_sales_and_returns(sales_df, returns_df):
    '''We match the most recent sale for each return'''
    returned_rows = []
    used_rows = set()
    counter = 0
    for i, r in returns_df.iterrows():
        print('Processing Return:', counter)
        counter += 1
        prior_sales = sales_df[(sales_df['date'] < r['date']) &
                               (sales_df['cx_num'] == r['cx_num']) &
                               (sales_df['mat_id'] == r['mat_id']) &
                               (~sales_df.index.isin(used_rows))
                               ]
        if not prior_sales.empty:
            last_idx = prior_sales.index[0]
            returned_rows.append(last_idx)  # most recent sale
            used_rows.add(last_idx)
        else:
            continue
    unmatched_returns = len(returns_df) - len(returned_rows)
    print(f"Unmatched Returns: {unmatched_returns:,}")
    print(f"Netted out {len(returned_rows):,} returned sales from SOV data.")
    return returned_rows

print(f"Before Removing Returns:\n   QTY SOLD = {sales['qty'].sum():,.2f},\n   TOTAL REVENUE = {sales['val'].sum():,.2f}")
returned_indices = match_sales_and_returns(sales, returns)
sales = sales.drop(index=returned_indices)
print(f"After Removing Returns:\n   QTY SOLD = {sales['qty'].sum():,.0f},\n   TOTAL REVENUE = {sales['val'].sum():,.2f}")


