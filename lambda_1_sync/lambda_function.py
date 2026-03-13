import boto3
import urllib.request
import re
import os
import json
from datetime import datetime

s3 = boto3.client("s3")
BUCKET = os.environ.get('BUCKET_NAME')
BASE_URL = "https://download.bls.gov/pub/time.series/pr/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

def make_request(url, method='GET'):
    req = urllib.request.Request(url, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            return response.read(), dict(response.info()), response.status
    except Exception as e:
        print(f"Request failed for {url}: {e}")
        return None, None, 500

def list_source_files():
    body, _, _ = make_request(BASE_URL)
    html = body.decode('utf-8')
    hrefs = re.findall(r'HREF="([^"]+)"', html, flags=re.IGNORECASE)
    return [h.split("/")[-1] for h in hrefs if "/pub/time.series/pr/" in h and not h.endswith("/")]

def sync_to_s3(file_list):
    for filename in file_list:
        file_url = f"{BASE_URL}{filename}"
        s3_key = f"time_series/pr/{filename}"
        
        # 1. Get Source Metadata using HEAD
        _, headers, _ = make_request(file_url, method='HEAD')
        source_size = int(headers.get('Content-Length', 0))
        
        # Parse Last-Modified (e.g., "Thu, 29 Jan 2026 13:30:00 GMT")
        source_last_mod_str = headers.get('Last-Modified')
        source_last_mod = None
        if source_last_mod_str:
            try:
                source_last_mod = datetime.strptime(source_last_mod_str, "%a, %d %b %Y %H:%M:%S %Z")
            except ValueError:
                pass 

        # 2. Check S3 Metadata
        try:
            s3_meta = s3.head_object(Bucket=BUCKET, Key=s3_key)
            s3_size = s3_meta['ContentLength']
            s3_last_mod = s3_meta['LastModified'].replace(tzinfo=None) # Make naive for comparison
            
            if source_size == s3_size and source_last_mod and s3_last_mod >= source_last_mod:
                print(f"Skipping {filename} : Already up to date.")
                continue
            else:
                print(f"Updating {filename} : Changes detected...")
        except s3.exceptions.ClientError:
            print(f"Uploading {filename} : New file...")

        # 3. Download and Upload
        body, _, status = make_request(file_url)
        if status == 200:
            s3.put_object(Bucket=BUCKET, Key=s3_key, Body=body)
            print(f"Successfully synced {filename}")

def cleanup_s3(source_file_list):
    prefix = "time_series/pr/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    
    if 'Contents' not in response:
        return
        
    s3_files = {obj['Key'].replace(prefix, "") for obj in response['Contents']}
    files_to_delete = s3_files - set(source_file_list)
    
    for filename in files_to_delete:
        s3.delete_object(Bucket=BUCKET, Key=f"{prefix}{filename}")
        print(f"Deleted orphaned file: {filename}")

def sync_population_data():
    pop_api_url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    s3_key = "population_data/us_population.json"
    
    body, _, status = make_request(pop_api_url)
    if status == 200:
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=body)
        print(f"Successfully saved population data.")

def lambda_handler(event, context):
    print("--- Starting Part 1: BLS Sync ---")
    files_to_sync = list_source_files()
    sync_to_s3(files_to_sync)
    cleanup_s3(files_to_sync)
    
    print("--- Starting Part 2: Population API ---")
    sync_population_data()
    
    return {'statusCode': 200, 'body': 'Part 1 and 2 sync complete!'}