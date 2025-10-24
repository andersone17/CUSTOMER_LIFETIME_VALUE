# Script to Map Sales Data with Master Data (Both Gear and Bike)

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


# Load Master Data Files
bike_master = pd.read_excel(r"DATA\master_data_zfer.xlsx")
gear_master = pd.read_excel(r"DATA\master_data_gear.xlsx")

# Clean Bike Master
bike_master.rename(columns={'Material Number': 'mat_id',
                            'KMAT Number': 'kmat', 
                            'Product Group' : 'model'}, inplace=True)
bike_master.columns = [col.lower().replace(' ', '_') for col in bike_master.columns]
bike_master = bike_master[['mat_id', 'kmat', 'model', 'platform', 'family', 
                           'drivetrain', 'world']]
bike_master['mat_id'] = pd.to_numeric(bike_master['mat_id'], errors='coerce')
bike_master['kmat'] = pd.to_numeric(bike_master['kmat'], errors='coerce')
bike_master['my'] = bike_master['model'].str[:4].str.strip()
bike_master['my'] = pd.to_numeric(bike_master['my'], errors='coerce')
bike_master['model'] = bike_master['model'].str[4:].str.strip()

# Clean Gear Master
gear_master.rename(columns={'Material Number': 'mat_id',
                             'KMAT Number': 'kmat', 
                             'Product Group' : 'model'}, inplace=True)
gear_master.columns = [col.lower().replace(' ', '_') for col in gear_master.columns]
gear_master = gear_master[['mat_id', 'kmat', 'model', 'platform', 'family', 
                           'world']]

#####################################################################################
# Merge Data with Bike Master
def map_sov_data(input_path, output_path):
    '''Function to clean and merge sales data with bike and gear master data.'''
    data = pd.read_csv(input_path)
    data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d', errors='coerce')
    data['mat_id'] = pd.to_numeric(data['mat_id'], errors='coerce')
    data['kmat'] = pd.to_numeric(data['kmat'], errors='coerce')

    final_data = data.copy()
    final_cols = ['final_model', 'final_my', 'final_platform',
                'final_family', 'final_drivetrain', 'final_world']
    for col in final_cols:
        final_data[col] = np.nan
    # Merge on ZFER
    final_data = pd.merge(final_data, bike_master[['mat_id', 'model', 'my', 'platform', 
                                                'family', 'drivetrain', 'world']], 
                        on='mat_id', 
                        how='left')
    for col in final_cols:
        final_data[col] = np.where(final_data['model'].notna(), 
                                final_data[col.replace('final_', '')], 
                                final_data[col])
    final_data.drop(columns=['model', 'my', 'platform', 'family',
                            'drivetrain', 'world'], inplace=True)
    # Merge on KMAT
    kmat_master = bike_master.groupby('kmat').first().reset_index()
    final_data = pd.merge(final_data, kmat_master[['kmat', 'model', 'my', 'platform', 
                                                'family', 'drivetrain', 'world']], 
                        on='kmat', 
                        how='left')
    for col in final_cols:
        final_data[col] = np.where((final_data['model'].notna()) & (final_data[col].isna()),
                                final_data[col.replace('final_', '')],
                                final_data[col])
    final_data['type'] = np.where(final_data['final_model'].notna(), 'Bike', np.nan)
    final_data.drop(columns=['model', 'my', 'platform', 'family',
                            'drivetrain', 'world'], inplace=True)

    # Merge Data with Gear Master on ZFER
    final_data = pd.merge(final_data, gear_master[['mat_id', 'model', 'platform', 
                                                'family', 'world']], 
                        on='mat_id', 
                        how='left')
    final_cols = ['final_model', 'final_platform', 'final_family', 'final_world']
    for col in final_cols:
        final_data[col] = np.where((final_data['model'].notna()) & (final_data[col].isna()), 
                                final_data[col.replace('final_', '')], 
                                final_data[col])
    final_data.drop(columns=['model', 'platform', 'family', 'world'], inplace=True)
    final_data.loc[(final_data['final_model'].notna()) & (final_data['type'] == "nan"), 'type'] = 'Gear'
    final_data.to_csv(output_path, index=False)
    return final_data


map_sov_data(input_path=r'DATA\cleaned_data\cleaned_data.csv',
              output_path=r'DATA\cleaned_data\mapped_sov.csv')
map_sov_data(input_path=r'DATA\cleaned_data\netted_sales_data.csv',
              output_path=r'DATA\cleaned_data\mapped_net_sales_data.csv')
print("Mapping Completed and Files Saved.")