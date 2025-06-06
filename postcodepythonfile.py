import pandas as pd
import requests
import math
from tqdm import tqdm

def get_bulk_data(postcodes):
    url = "https://api.postcodes.io/postcodes"
    headers = {"Content-Type": "application/json"}
    payload = {"postcodes": postcodes}

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result_list = response.json()['result']

        output = {}
        for item in result_list:
            pc = item['query']
            result = item.get('result')
            output[pc] = result if result else {}
        return output
    except Exception as e:
        print(f"Bulk API error: {e}")
        return {pc: {} for pc in postcodes}

def enrich_postcodes_full_data():
    input_file = 'PostcodesFile.xlsx'
    output_file = 'PostcodesFile_with_full_data.xlsx'
    postcode_column = 'Postcode'

    df = pd.read_excel(input_file)
    df['Postcode_cleaned'] = df[postcode_column].astype(str).str.strip().str.upper()
    postcode_list = df['Postcode_cleaned'].tolist()

    batch_size = 100
    enriched_data = []

    print(f"Fetching full data for {len(postcode_list)} postcodes...")

    for i in tqdm(range(0, len(postcode_list), batch_size)):
        batch = postcode_list[i:i+batch_size]
        batch_data = get_bulk_data(batch)
        for pc in batch:
            row = batch_data.get(pc, {})
            row['Postcode_cleaned'] = pc
            enriched_data.append(row)

    # Convert list of dicts to DataFrame
    enriched_df = pd.DataFrame(enriched_data)

    # Merge back into original
    merged_df = pd.merge(df, enriched_df, how='left', on='Postcode_cleaned')
    merged_df.drop(columns=['Postcode_cleaned'], inplace=True)

    merged_df.to_excel(output_file, index=False)
    print(f"\n✅ Done. Full postcode data written to: {output_file}")

# Run it
if __name__ == "__main__":
    enrich_postcodes_full_data()
