import pandas as pd

FILE_INTERNET = 'Internet Penetration.csv'  
FILE_SERVERS  = 'Secure Servers.csv'  
FILE_NDGAIN   = 'ND-GAIN Readiness score.csv' 

arab_states = [
    'DZA', 'BHR', 'COM', 'DJI', 'EGY', 'IRQ', 'JOR', 'KWT', 'LBN', 'LBY', 
    'MRT', 'MAR', 'OMN', 'PSE', 'QAT', 'SAU', 'SOM', 'SDN', 'SYR', 'TUN', 
    'ARE', 'YEM'
]

country_map = {
    'DZA': 'Algeria', 'BHR': 'Bahrain', 'COM': 'Comoros', 'DJI': 'Djibouti', 
    'EGY': 'Egypt', 'IRQ': 'Iraq', 'JOR': 'Jordan', 'KWT': 'Kuwait', 
    'LBN': 'Lebanon', 'LBY': 'Libya', 'MRT': 'Mauritania', 'MAR': 'Morocco', 
    'OMN': 'Oman', 'PSE': 'Palestine', 'QAT': 'Qatar', 'SAU': 'Saudi Arabia', 
    'SOM': 'Somalia', 'SDN': 'Sudan', 'SYR': 'Syria', 'TUN': 'Tunisia', 
    'ARE': 'United Arab Emirates', 'YEM': 'Yemen'
}

def process_wide_file(filepath, value_name):
    # 'utf-8-sig' strips the invisible Excel BOM character
    df = pd.read_csv(filepath, encoding='utf-8-sig')
    
    # Strip any accidental hidden spaces from the column headers
    df.columns = df.columns.astype(str).str.strip()
    
    # Filter down to the 22 Arab States
    df = df[df['Country Code'].isin(arab_states)]
    
    # Melt the wide years into a single column
    df_long = df.melt(id_vars='Country Code', var_name='Year', value_name=value_name)
    
    # Keep only the columns that are actual years (ignores metadata like 'Country Name')
    df_long = df_long[df_long['Year'].astype(str).str.isnumeric()]
    df_long['Year'] = df_long['Year'].astype(int)
    
    return df_long[['Country Code', 'Year', value_name]].dropna()

print("Melting wide datasets...")
df_internet = process_wide_file(FILE_INTERNET, 'Internet_Penetration_Pct')
df_servers  = process_wide_file(FILE_SERVERS, 'Secure_Servers_Per_1M')
df_ndgain   = process_wide_file(FILE_NDGAIN, 'ND_GAIN_Readiness')

print("\n--- FINAL ROW COUNTS ---")
print(f"Internet data: {len(df_internet)} rows")
print(f"Servers data: {len(df_servers)} rows")
print(f"ND-GAIN data: {len(df_ndgain)} rows")

print("\nMerging into master dataset...")
master_df = pd.merge(df_ndgain, df_internet, on=['Country Code', 'Year'], how='inner')
master_df = pd.merge(master_df, df_servers, on=['Country Code', 'Year'], how='inner')

# Filter for the relevant decade+ of data
master_df = master_df[master_df['Year'] >= 2010]

# Add clean country names for Tableau mapping
master_df['Country_Name'] = master_df['Country Code'].map(country_map)

# Final column order for a professional export
master_df = master_df[['Country Code', 'Country_Name', 'Year', 'ND_GAIN_Readiness', 'Internet_Penetration_Pct', 'Secure_Servers_Per_1M']]

master_df.to_csv('tableau_climate_tech_master.csv', index=False)
print("\nSuccess! Master dataset generated. Total merged rows:", len(master_df))
