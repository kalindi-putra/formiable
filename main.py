import requests
import json
import time
import psycopg2
import datetime
from io import BytesIO
import re
from bs4 import BeautifulSoup
import os
from urllib.parse import quote_plus, urlparse
from warcio.archiveiterator import ArchiveIterator

SERVER = 'http://index.commoncrawl.org/'
INDEX_NAME = 'CC-MAIN-2025-05'

DB_HOST = os.environ.get('DB_HOST')
DB_PORT = int(os.environ.get('DB_PORT'))
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

target_urls = [
    "com.au", "net.au", "org.au", "gov.au", "edu.au", "asn.au", "biz.au", "id.au", "csiro.au",
]

myagent = 'cc-get-started/1.0 (Example data retrieval script; cactusuncle8@gmail.com)'

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def search_cc_index(url):
    encoded_url = quote_plus(url)
    index_url = f'{SERVER}{INDEX_NAME}-index?url={encoded_url}&output=json'
    print(index_url)
    response = requests.get(index_url, headers={'user-agent': myagent}, verify=False)
    print("Response from server:\r\n", response.text)
    if response.status_code == 200:
        records = response.text.strip().split('\n')
        return [json.loads(record) for record in records]
    else:
        return None

def extract_abn(content):
    abn_pattern = r'\b\d{2}\s?\d{3}\s?\d{3}\s?\d{3}\b'
    
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            text = content.decode('latin-1')
        except:
            return None
    
    abn_matches = re.findall(abn_pattern, text)
    if abn_matches:
        abn = re.sub(r'\s', '', abn_matches[0])
        return abn
    return None

def extract_business_details(content, url):
    try:
        soup = BeautifulSoup(content, 'html.parser')
        company_name = soup.title.string if soup.title else None
        
        return {
            'company_name': company_name,
            'company_url': url
        }
    except:
        return {
            'company_name': None,
            'company_url': url
        }

def fetch_page_from_cc(records, url):
    for record in records:
        time.sleep(10)
        offset, length = int(record['offset']), int(record['length'])
        s3_url = f'https://data.commoncrawl.org/{record["filename"]}'
        byte_range = f'bytes={offset}-{offset+length-1}'

        response = requests.get(
            s3_url,
            headers={'user-agent': myagent, 'Range': byte_range},
            stream=True,
            verify=False
        )

        if response.status_code == 206:
            stream = ArchiveIterator(response.raw)
            for warc_record in stream:
                if warc_record.rec_type == 'response':
                    print(f"Found data for {url}")
                    content = warc_record.content_stream().read()
                    
                    warc_headers = warc_record.rec_headers
                    http_headers = warc_record.http_headers
                    
                    meta = {
                        'url_key': record.get('urlkey', ''),
                        'company_url': url,
                        'file_name': record.get('filename', ''),
                        'mime': http_headers.get_header('content-type', '') if http_headers else '',
                        'encoding': http_headers.get_header('content-encoding', '') if http_headers else '',
                        'length': record.get('length', 0),
                        'offset': record.get('offset', 0),
                        'digest': record.get('digest', ''),
                        'mime_detected': record.get('mime-detected', ''),
                        'languages': record.get('languages', ''),
                        'timestamp': record.get('timestamp', '')
                    }
                    
                    business_details = extract_business_details(content, url)
                    if business_details['company_name']:
                        meta['company_name'] = business_details['company_name']
                    
                    abn = extract_abn(content)
                    
                    save_to_database(meta, abn, content)
                    
                    return content
        else:
            print(f"Failed to fetch data: {response.status_code}")
    
    print(f"No valid WARC record found for {url}")
    return None

def save_to_database(metadata, abn, content):
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        data_load_date = datetime.datetime.now()
        
        cursor.execute("""
            INSERT INTO stg_business_details 
            (url_key, company_url, company_name, file_name, mime, encoding, 
             length, offset, digest, mime_detected, languages, timestamp, data_load_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url_key) DO UPDATE SET
            company_url = EXCLUDED.company_url,
            company_name = EXCLUDED.company_name,
            data_load_date = EXCLUDED.data_load_date
        """, (
            metadata.get('url_key', ''),
            metadata.get('company_url', ''),
            metadata.get('company_name', ''),
            metadata.get('file_name', ''),
            metadata.get('mime', ''),
            metadata.get('encoding', ''),
            metadata.get('length', 0),
            metadata.get('offset', 0),
            metadata.get('digest', ''),
            metadata.get('mime_detected', ''),
            metadata.get('languages', ''),
            metadata.get('timestamp', ''),
            data_load_date
        ))
        
        if abn:
            entity_name = metadata.get('company_name', '')
            
            cursor.execute("""
                INSERT INTO stg_abn_details 
                (ABN, entity_name, entity_type, registration_date, state, post_code, status, ACN, GST, data_load_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ABN) DO UPDATE SET
                entity_name = EXCLUDED.entity_name,
                data_load_date = EXCLUDED.data_load_date
            """, (
                int(abn),
                entity_name,
                'Unknown',
                None,
                None,
                None,
                'Active',
                None,
                None,
                data_load_date
            ))
        
        conn.commit()
        print(f"Successfully saved data for {metadata.get('company_url', '')}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error saving to database: {e}")
    finally:
        cursor.close()
        conn.close()

def create_tables_if_not_exist():
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_business_details (
                url_key VARCHAR(255) PRIMARY KEY,
                company_url VARCHAR(255),
                company_name VARCHAR(255),
                file_name VARCHAR(255),
                mime VARCHAR(100),
                encoding VARCHAR(50),
                length INTEGER,
                offset INTEGER,
                digest VARCHAR(100),
                mime_detected VARCHAR(100),
                languages VARCHAR(50),
                timestamp TIMESTAMP,
                data_load_date TIMESTAMP
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_abn_details (
                ABN INTEGER PRIMARY KEY,
                entity_name VARCHAR(255),
                entity_type VARCHAR(100),
                registration_date DATE,
                state VARCHAR(50),
                post_code VARCHAR(10),
                status VARCHAR(50),
                ACN VARCHAR(50),
                GST VARCHAR(50),
                data_load_date TIMESTAMP
            );
        """)
        
        conn.commit()
        print("Database tables created or already exist")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating tables: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    create_tables_if_not_exist()
    
    for target_url in target_urls:
        print(f"Processing {target_url}...")
        records = search_cc_index(target_url)
        
        if records:
            print(f"Found {len(records)} records for {target_url}")
            content = fetch_page_from_cc(records, target_url)
            
            if content:
                try:
                    os.makedirs("outputs", exist_ok=True)
                    
                    file_path = f"outputs/{target_url.replace('.', '_')}.html"
                    with open(file_path, "wb") as file:
                        file.write(content)
                        print(f"Saved content to {file_path}")
                except Exception as e:
                    print(f"Error saving file: {e}")
        else:
            print(f"No records found for {target_url}")

if __name__ == "__main__":
    main()