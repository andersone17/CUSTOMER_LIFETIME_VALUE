# Exploratory Analysis of Customer Lifetime Value Data


import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

#INPUTS
PATH = r"DATA\cleaned_data\mapped_net_sales_data.csv"

data = pd.read_csv(PATH)
data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d', errors='coerce')
data['mat_id'] = pd.to_numeric(data['mat_id'], errors='coerce')
data['kmat'] = pd.to_numeric(data['kmat'], errors='coerce')

for i, c in enumerate(data['cx_num'].unique()):
    print(f'Iteration {i+1}: Customer {c}')
    subset = data[data['cx_num'] == c].sort_values(by='date')
    min_date = subset['date'].min()
    max_date = subset['date'].max()
    ecc_start = datetime(2023, 12, 31)
    if min_date < ecc_start and max_date > ecc_start:
        break

# This confirms that BYD CX NUMS carry over to ECC!!!!!!!!!!!!!!!!!!
subset














# OUTLINE:

# Contingency Table to Assess Most Frequent Followup Purchase 
    # For Gear
    # For Bikes

# Assess Follow Up Purchase Timing
    # Does it differ by product category?
    # Differ by price?
    # Does it differ by customer segment?

# Visualizations
    # Histograms of follow up purchase timing
    


# Family platform level what is the lifetime value of those purchases?
# Lifetime Value is the total revenue from a customer over a defined period (e.g., 1 year, 3 years, 5 years).
    # total_revenue/time/total_customers_retained
    # Avg Purchase Val - Avg Purchase Freq x Avg Customer Lifespan
    # 50 dollars - 3 purchases per year x 5 years = 750 dollars
    
# Repeat purchase rate

# Research Lifetime Customer Value analysis and cohort analysis methods
# Cohort analysis to see how customer retention changes.