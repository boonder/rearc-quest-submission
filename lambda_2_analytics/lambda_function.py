import json
import boto3
import pandas as pd
import os

s3 = boto3.client('s3')
BUCKET = os.environ.get('BUCKET_NAME')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}") 
    print("Starting analytics...")
    
    # --- 1. Load BLS Data ---
    print("Loading BLS Data from S3...")
    bls_obj = s3.get_object(Bucket=BUCKET, Key='time_series/pr/pr.data.0.Current')
    # Read CSV, using tab separator and stripping whitespace from headers
    df_bls = pd.read_csv(bls_obj['Body'], sep='\t')
    df_bls.columns = df_bls.columns.str.strip() 
    df_bls['series_id'] = df_bls['series_id'].str.strip()
    df_bls['period'] = df_bls['period'].str.strip()
    df_bls['value'] = pd.to_numeric(df_bls['value'], errors='coerce')

    # --- 2. Load Population Data ---
    print("Loading Population Data from S3...")
    pop_obj = s3.get_object(Bucket=BUCKET, Key='population_data/us_population.json')
    pop_data = json.loads(pop_obj['Body'].read().decode('utf-8'))
    
    # Flatten the data array
    df_pop = pd.json_normalize(pop_data['data'])
    df_pop['Year'] = df_pop['Year'].astype(int)
    df_pop['Population'] = df_pop['Population'].astype('int64')

    # ANALYTICS Step 1: Mean & StdDev of Population (2013-2018)
    pop_filtered = df_pop[(df_pop['Year'] >= 2013) & (df_pop['Year'] <= 2018)]
    mean_pop = pop_filtered['Population'].mean()
    std_pop = pop_filtered['Population'].std()
    
    print("\n--- 1. Population Stats (2013-2018) ---")
    print(f"Mean: {mean_pop}")
    print(f"StdDev: {std_pop}")

    # ANALYTICS Step 2: Best Year for Every Series
    # Group by series and year, sum the values
    yearly_sums = df_bls.groupby(['series_id', 'year'])['value'].sum().reset_index(name='total_value')
    # Find the index of the maximum value for each series
    idx = yearly_sums.groupby('series_id')['total_value'].idxmax()
    best_years = yearly_sums.loc[idx]
    
    print("\n--- 2. Best Year Per Series (Top 5) ---")
    print(best_years.head(5).to_string(index=False))

    # ANALYTICS Step 3: Target Series vs Population
    target_bls = df_bls[(df_bls['series_id'] == 'PRS30006032') & (df_bls['period'] == 'Q01')]
    # Join on year
    merged_df = pd.merge(target_bls, df_pop, left_on='year', right_on='Year', how='inner')
    final_report = merged_df[['series_id', 'year', 'period', 'value', 'Population']]
    
    print("\n--- 3. Final Report (Target Series & Population) ---")
    print(final_report.to_string(index=False))

    return {
        'statusCode': 200,
        'body': json.dumps('Analytics Completed Successfully!')
    }