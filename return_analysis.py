#SASKIAS RETURN ANALYSIS

import pandas as pd
import numpy as np
import os
import plotly.express as px
import matplotlib.pyplot as plt
import webbrowser
#import folium
#import pgeocode
#import geopandas


#INPUTS
PLATFORMS = [' SLX ', ' CFR ', ' CF ', ' AL '] #<-- Whitespace is needed
FAMILY_MAPPING = {
    "GRZ[ON]FLY": "E-Gravel",
    "GRZ[ONFLY]": "E-Gravel",    
    "GRC[ON]": "E-MTB",
    "PTL[ON]": "E-Urban/Fitness",
    "STV[ON]": "E-MTB",
    "EDR[ON]": "E-Road",
    "NRN[ON]FLY": "E-MTB",
    "NRN[ONFLY]": "E-MTB",
    "SPT[ON]FLY": "E-MTB",
    "SPT[ONFLY]": "E-MTB",
    "LUX:ONFLY": "E-MTB",
    "LUX[ON]FLY": "E-MTB",
    "LUX[ONFLY]": "E-MTB",
    "SPT[ON]FLY": "E-MTB",
    "SPT[ON]": "E-MTB",
    "GRL[ON]": "E-Gravel",
    "PRE[ON]": "E-Urban/Fitness",
    "TRQ[ON]": "E-MTB",
    "ARO": "Road",
    "CMT": "Urban/Fitness",
    "DUD": "MTB",
    "EDR": "Road",
    "EXD": "MTB",
    "GRC": "MTB",
    "GRL": "Gravel",
    "GRZ": "Gravel",
    "IFL": "Road",
    "LUXWC": "MTB",
    "LUX": "MTB",
    "NRN": "MTB",
    "PTL": "Urban/Fitness",
    "RDL": "Urban/Fitness",
    "SDR": "MTB",
    "SPM": "Road",
    "SPT YH": "MTB",
    "SPT125": "MTB",
    "SPT": "MTB",
    "STC": "MTB",
    "STO": "MTB",
    "STV": "MTB",
    "TRQ": "MTB",
    "ULT": "Road",
    "VAR": "VAR",
    "ACC": "ACC",
    }

fam_remap = {
    "NRN[ONFLY]" : "NRN[ON]FLY", 
    'SPT[ONFLY]': 'SPT[ON]FLY',
    'LUX[ONFLY]': 'LUX[ON]FLY',
    'LUX:ONFLY': 'LUX[ON]FLY',
    'GRZ[ONFLY]': 'GRZ[ON]FLY',
    'SPT YH' : 'SPT'}
    

#FILE I/O PATHS
SALES_ORDER_PATH = r"A:\Return Analysis\sales_orders_20240101_20250515.xlsx"
RETURNS_PATH = r"A:\Return Analysis\returns_20240101_20250515.xlsx"

#Create Folder to Store Data and Graphs
path = "A:\\Return Analysis\\Analysis_20250515"
if not os.path.exists(path):
    os.mkdir(path)
output_path = f"{path}\\Clean_Data_{path[-8:]}.xlsx"    

#####################################################################################################
#####################################################################################################
                                    #DATA CLEANING AND PROCESSING
#LOAD AND CLEAN SALES DATA:
    # Clean address, state, zip, date, sales num, zfer, etc. 
    # Ensure that Family and Platform exists in the data
        # If not, map with master data off zfer 
    # Split the data into orders and returns
sales_data = pd.read_excel(SALES_ORDER_PATH)
sales_cols = {
    "SD Document":"Sales Order", 
    'Created On':'Order Date', 
    'Delivery Date':'Delivery Date',
    'Cancellation Date':'Cancelled Date',
    'Shipping condition' : 'Shipping Type', 
    'Postal Code':'ZIP', 
    'City':'City', 
    'Region':'State',
    'Country':'Country', 
    'Terms of payment':'Payment Type',
    'Order Quantity':'Order Quantity',
    'Order value (gross)':'Gross Value', 
    'Net value':'Net Value',
    'Material':'Material ID', 
    'Short txt customer order item': 'Description',
    'Batch':'Batch'
    }
sales_data = sales_data[[col for col in sales_cols.keys()]]
sales_data.rename(columns=sales_cols, inplace=True)
    #filter out cancelations
sales_data[['Order Date', 'Delivery Date', 'Cancelled Date']] = \
    sales_data[['Order Date', 'Delivery Date', 'Cancelled Date']].apply(pd.to_datetime, errors='coerce')
sales_data = sales_data[sales_data['Cancelled Date'].isna()]
    #filter out bstock
sales_data[['Sales Order', 'Order Quantity', 'Gross Value', 'Net Value', 'Material ID', 'Batch']] = \
    sales_data[['Sales Order', 'Order Quantity', 'Gross Value', 'Net Value', 'Material ID', 'Batch']] \
        .apply(pd.to_numeric, errors='coerce')
sales_data = sales_data[sales_data['Batch'].isna()]
    #Map Description to Fam and Platform
sales_data['Description'] = sales_data['Description'].astype(str).str.upper()
sales_data['FAM'] = sales_data['Description']\
    .apply(lambda x: next((fam for fam in FAMILY_MAPPING.keys() if fam in x), np.nan))
sales_data['FAM'] = sales_data['FAM'].replace(fam_remap)
sales_data['WORLD'] = sales_data['FAM'].map(FAMILY_MAPPING)
sales_data['PLAT'] = sales_data['Description']\
    .apply(lambda x: next((plat for plat in PLATFORMS if plat in x), "AL"))
sales_data['PLAT'] = sales_data['PLAT'].astype(str).str.strip()
    #Clean Address Data
sales_data['ZIP'] = sales_data['ZIP'].astype(str).str.split('-').str[0]
sales_data['ZIP'] = pd.to_numeric(sales_data['ZIP'], errors='coerce')
sales_data['City'] = sales_data['City'].astype(str).str.strip().str.title()
sales_data['State'] = sales_data['State'].astype(str).str.strip().str.upper()
sales_data['Country'] = sales_data['Country'].astype(str).str.strip().str.upper()
    #random cleaning
sales_data['Payment Type'] = sales_data['Payment Type'].astype(str).str.strip().str.upper()
sales_data.drop(columns=['Cancelled Date', 'Batch', 'Gross Value'], inplace=True)
sales_data = sales_data[sales_data['Sales Order'].astype(str).str.startswith("392")]
sales_data = sales_data.drop_duplicates(subset=['Sales Order', 'Material ID'])
    #Remove urban and eurban from data
sales_data = sales_data[(sales_data['WORLD'] != 'Urban/Fitness') &
                        (sales_data['WORLD'] != 'E-Urban/Fitness')]

#LOAD AND CLEAN RETURN DATA: clean in similar way 
   # then merge onto sales data with material and sales order
return_data = pd.read_excel(RETURNS_PATH)
return_cols_dict = {
    'Sales Order #':'Sales Order', 
    'Return ID':'Return ID',
    'Created On':'Return Date', 
    'Sales order type':'Sales Order Type',
    'Customer' :'Customer ID', 
    'Material':'Material ID', 
    'Cancellation Date': 'Cancellation Date', 
    'Reason for rejection':'Return Code', 
    'Cancellation reason':'Cancellation Reason'    
}
return_data = return_data[[col for col in return_cols_dict.keys()]]
return_data.rename(columns=return_cols_dict, inplace=True)
return_data[['Sales Order', 'Return ID', 'Material ID', 'Customer ID']] = \
    return_data[['Sales Order', 'Return ID', 'Material ID', 'Customer ID']] \
        .apply(pd.to_numeric, errors='coerce')
return_data['Return Date'] = pd.to_datetime(return_data['Return Date'], errors='coerce')
return_data = return_data[return_data['Cancellation Date'].isna()]
return_data = return_data.drop_duplicates(subset=['Customer ID', 'Material ID'])
return_data.drop(columns=['Cancellation Date', 'Return Code', 'Cancellation Reason'], 
                 inplace=True)

#MERGE RETURNS ONTO SALES ORDERS:
sales_data = pd.merge(sales_data, 
                      return_data[['Sales Order', 'Material ID', 
                                   'Return Date', 'Return ID']], 
                      on=['Sales Order', 'Material ID'], 
                      how='left'
                      )
sales_data['RETURN FLAG'] = np.where(sales_data['Return ID'].notna(), 
                                     1, 
                                     0)


###########################################################################################
###########################################################################################
                                #CALCULATE RETURN RATES:
    # Lets look at return rates at various aggregation level... 
    # Family level return rates and total returns -- price sensitivity
    # Family-Platform level return rates and total returns
    # Platform only level return rates and total returns
    # State level return rates and total returns (compare heatmaps)
    # Zip code level return rates and total returns

def return_rate(df_input, grouping_list):
    df = df_input.groupby(grouping_list).agg({
        'Order Quantity':'sum', 
        'RETURN FLAG' : 'sum'
    }).reset_index().rename(columns={'RETURN FLAG':'Returns'})
    df['RETURN RATE'] = (df['Returns'] / df['Order Quantity'])*100
    return df

fam_returns = return_rate(sales_data, ['FAM', 'WORLD'])
plat_returns = return_rate(sales_data, ['PLAT'])
fam_plat_returns = return_rate(sales_data, ['FAM', 'PLAT', 'WORLD'])
state_returns = return_rate(sales_data, ['State'])
zip_returns = return_rate(sales_data, ['ZIP'])

#Time to return analysis
def time_to_return(df, platform_filter=False):
    if platform_filter:
        df = df[df['PLAT'] == platform_filter]
    df['Days Elapsed'] = (df['Return Date'] - df['Delivery Date']).dt.days
    df = df[['Delivery Date', 'Return Date', 'Days Elapsed']]
    df = df[(df['Days Elapsed'] < 40) & (df['Days Elapsed'] >= 0)]
    df.sort_values(by='Days Elapsed', ascending=True, inplace=True)
    return df

time_to_return_all = time_to_return(sales_data)
time_to_return_al = time_to_return(sales_data, 'AL')
time_to_return_cf = time_to_return(sales_data, 'CF')
time_to_return_slx = time_to_return(sales_data, 'SLX')
time_to_return_cfr = time_to_return(sales_data, 'CFR')


#SAVE DATA TO OUTPUT PATH
with pd.ExcelWriter(output_path) as writer:
    sales_data.to_excel(writer, sheet_name='UNDERLYING_DATA', index=False)
print('Underlying data exported')
    


#####################################################################################
#####################################################################################
                                    #VISUALIZATIONS:
# Histogram: Avg time to return
def time_to_return_histogram(hist_data, platform=False):
    BINS = int(hist_data['Days Elapsed'].max())
    XTICKS = sorted(hist_data['Days Elapsed'].unique().tolist())
    plt.style.use('seaborn-v0_8-white')
    fig, ax = plt.subplots(figsize=(10,7), dpi=200)
    ax.hist(hist_data['Days Elapsed'], bins=BINS,
            color='#ed742e', edgecolor='black', alpha = .7)
    ax.set_ylabel('Return Qty (Bike Only)', fontweight = 'bold')
    ax.set_xlabel('Days Elapsed from Delivery Date' , fontweight = 'bold')
    ax.set_xticks(XTICKS)
    ax.set_xticklabels([f"{int(x):,.0f}" for x in sorted(hist_data['Days Elapsed'].unique().tolist())], rotation=0)
    ax.tick_params(axis='x', labelsize=11)
    if platform:
        plt.title(f'Days-to-Return ({platform})', 
                  fontweight = 'bold', fontsize = 15)
    else:
        plt.title('Days-to-Return all bikes (Jan-2024 to May-2025)', 
                  fontweight = 'bold', fontsize = 15)
    plt.tight_layout()
    if platform:
        plt.savefig(f"{path}\\days_to_return_hist_{platform}.png")
    else:
        plt.savefig(f"{path}\\days_to_return_hist_all_plats.png")

time_to_return_histogram(time_to_return_all)
time_to_return_histogram(time_to_return_al, 'AL')
time_to_return_histogram(time_to_return_cf, 'CF')
time_to_return_histogram(time_to_return_slx, 'SLX')
time_to_return_histogram(time_to_return_cfr, 'CFR')


    #Normalized to 100% easier with plotly
fig = px.histogram(
    time_to_return_all,
    x='Days Elapsed',
    nbins=20,
    histnorm='percent',
    title='Distribution of Returns by Days Elapsed (Jan-2024 to May-2025)',
    labels={
        'Time Elapsed': 'Days Between Order and Return',
        'percent': 'Return Share (%)'
        }, 
    color_discrete_sequence=['#ed742e']
)
fig.update_layout(
    yaxis_title='Return Share (%)', 
    title_x = .5)
fig.update_traces(marker_line_color='black', marker_line_width=1)
#fig.write_image(f"{path}\\days_to_return_percent_hist.png", scale=2)
fig.show()
fig.write_html("returns_histogram.html")
webbrowser.open("returns_histogram.html")



# BAR CHART: Top ten families by return rate and total returns (2 subplots/facets)
fam_returns.sort_values(by='RETURN RATE', ascending=False, inplace=True)
fam_total_returns = fam_returns.sort_values(by='Returns', ascending=False)

plt.style.use('seaborn-v0_8-white')
fig, (ax1, ax2) = plt.subplots(nrows = 2, figsize=(12,10), dpi=200)
fig.suptitle("Family Return Analysis (Jan-2024 to May-2025)", 
             fontsize=16, fontweight='bold')
ax1.bar(fam_returns['FAM'], fam_returns['RETURN RATE'], color = '#ed742e')
ax1.set_title('Return Rate by Family (%)', fontweight='bold')
ax1.set_ylabel('Return Rate (%)')
ax1.tick_params(axis="x", labelrotation=45)
for i,v in enumerate(fam_returns['RETURN RATE']):
    ax1.text(i, v, f"{v:.1f}%", ha='center', va='bottom')

ax2.bar(fam_total_returns['FAM'], fam_total_returns['Returns'], color = '#ed742e')
ax2.set_title('Total Returns by Family', fontweight='bold')
ax2.set_ylabel('Returns (Qty)')
ax2.tick_params(axis="x", labelrotation=45)
for i,v in enumerate(fam_total_returns['Returns']):
    ax2.text(i,v,f"{v:,.0f}", ha='center', va='bottom')
plt.tight_layout()
plt.savefig(f"{path}\\fam_return_analysis.png")
plt.show()



# BAR CHART: Returns by CF, SLX, CFR by return r and total returns (2 subplots/facets)
plat_returns.sort_values(by='RETURN RATE', ascending=False, inplace=True)
plat_total_returns = plat_returns.sort_values(by='Returns', ascending=False)

plt.style.use('seaborn-v0_8-white')
fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(12,10), dpi=200)
fig.suptitle('Platform Return Analysis (Jan-2024 to May-2025)', 
             fontsize=16, fontweight='bold')
ax1.bar(plat_returns['PLAT'], plat_returns['RETURN RATE'], color= '#ed742e')
ax1.set_title('Return Rate by Platform (%)', fontweight='bold')
ax1.set_ylabel('Return Rate (%)')
for i, v in enumerate(plat_returns['RETURN RATE']):
    ax1.text(i,v,f"{v:.1f}%", ha='center', va='bottom')
ax2.bar(plat_total_returns['PLAT'], plat_total_returns['Returns'], color='#ed742e')
ax2.set_title('Total Returns by Platform', fontweight='bold')
ax2.set_ylabel('Returns (Qty)')
for i,v in enumerate(plat_total_returns['Returns']):
    ax2.text(i,v,f"{v:,.0f}", ha='center', va='bottom')
plt.tight_layout()
plt.savefig(f"{path}\\platform_return_analysis.png")
plt.show()



# BAR CHART: Returns by FAM-PLAT combo. top ten of each. Rate and count (2 subplots stacked)
fam_plat_returns['FAM_PLAT'] = fam_plat_returns['FAM'] + " " + fam_plat_returns['PLAT']
fam_plat_returns.sort_values(by='RETURN RATE', ascending=False, inplace=True)
fam_plat_ret_total = fam_plat_returns.sort_values(by='Returns', ascending=False)
    #only top 15 records
fam_plat_returns = fam_plat_returns.iloc[0:15, :]
fam_plat_ret_total = fam_plat_ret_total.iloc[0:15, :]
    #graph
plt.style.use('seaborn-v0_8-white')
fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(12,10), dpi=200)
fig.suptitle('Top 15 Family/Platform Returns (Jan-2024 to May-2025)', 
             fontsize=16, fontweight='bold')
ax1.bar(fam_plat_returns['FAM_PLAT'], fam_plat_returns['RETURN RATE'], 
        color= '#ed742e')
ax1.set_title('Return Rate (%)', fontweight='bold')
ax1.set_ylabel('Return Rate (%)')
ax1.tick_params(axis="x", labelrotation=45)
for i, v in enumerate(fam_plat_returns['RETURN RATE']):
    ax1.text(i,v,f"{v:.1f}%", ha='center', va='bottom')
ax2.bar(fam_plat_ret_total['FAM_PLAT'], fam_plat_ret_total['Returns'], 
        color='#ed742e')
ax2.set_title('Total Returns', fontweight='bold')
ax2.set_ylabel('Returns (Qty)')
ax2.tick_params(axis="x", labelrotation=45)
for i,v in enumerate(fam_plat_ret_total['Returns']):
    ax2.text(i,v,f"{v:,.0f}", ha='center', va='bottom')
plt.tight_layout()
plt.savefig(f"{path}\\fam_plat_return_analysis.png")
plt.show()









































# OTHER IDEAS? : 
    # Return reasons?? -- sizing, defect, wrong product etc. 
    # 
    
 
 
 
 
 
    
