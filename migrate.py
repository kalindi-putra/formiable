import os
import csv
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

COMPANIES_CSV = "companies.csv"  #file in same directory
WEBSITES_CSV = "websites.csv"    #file in same directory

def create_tables(conn):
    """Create the required tables if they don't exist"""
    with conn.cursor() as cursor:
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                abn VARCHAR(20) PRIMARY KEY,
                entity_name VARCHAR(255),
                entity_type_code VARCHAR(10),
                entity_type VARCHAR(100),
                entity_status VARCHAR(50),
                entity_postcode VARCHAR(10),
                entity_state VARCHAR(20),
                entity_start_date DATE,
                asic_number VARCHAR(20),
                asic_type VARCHAR(50),
                gst_status VARCHAR(50),
                gst_from_date DATE,
                trading_names TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS websites (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255),
                website_url VARCHAR(255),
                industry VARCHAR(100)
            )
        """)
        
        conn.commit()
        print("Tables created successfully")

def import_companies_data(conn, csv_file):
    """Import data from companies CSV file to the companies table"""
    try:
        with conn.cursor() as cursor:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    entity_start_date = row['entity_start_date'] if row['entity_start_date'] else None
                    gst_from_date = row['gst_from_date'] if row['gst_from_date'] else None
                    
                    cursor.execute("""
                        INSERT INTO companies (
                            abn, entity_name, entity_type_code, entity_type, entity_status,
                            entity_postcode, entity_state, entity_start_date, asic_number,
                            asic_type, gst_status, gst_from_date, trading_names
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (abn) DO UPDATE SET
                            entity_name = EXCLUDED.entity_name,
                            entity_type_code = EXCLUDED.entity_type_code,
                            entity_type = EXCLUDED.entity_type,
                            entity_status = EXCLUDED.entity_status,
                            entity_postcode = EXCLUDED.entity_postcode,
                            entity_state = EXCLUDED.entity_state,
                            entity_start_date = EXCLUDED.entity_start_date,
                            asic_number = EXCLUDED.asic_number,
                            asic_type = EXCLUDED.asic_type,
                            gst_status = EXCLUDED.gst_status,
                            gst_from_date = EXCLUDED.gst_from_date,
                            trading_names = EXCLUDED.trading_names
                    """, (
                        row['abn'], row['entity_name'], row['entity_type_code'],
                        row['entity_type'], row['entity_status'], row['entity_postcode'],
                        row['entity_state'], entity_start_date, row['asic_number'],
                        row['asic_type'], row['gst_status'], gst_from_date,
                        row['trading_names']
                    ))
            
            conn.commit()
            print(f"Data imported successfully from {csv_file}")
    except Exception as e:
        conn.rollback()
        print(f"Error importing data from {csv_file}: {e}")

def import_websites_data(conn, csv_file):
    """Import data from websites CSV file to the websites table"""
    try:
        with conn.cursor() as cursor:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    cursor.execute("""
                        INSERT INTO websites (website_url, company_name, industry)
                        VALUES (%s, %s, %s)
                    """, (
                        row['Website URL'], row['Company Name'], row.get('Industry', None)
                    ))
            
            conn.commit()
            print(f"Data imported successfully from {csv_file}")
    except Exception as e:
        conn.rollback()
        print(f"Error importing data from {csv_file}: {e}")

def main():
    """Main function to coordinate the data import process"""
    try:
        
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        
        print("Connected to PostgreSQL database successfully")
        
        
        create_tables(conn)
        
        
        import_companies_data(conn, COMPANIES_CSV)
        import_websites_data(conn, WEBSITES_CSV)
        
        conn.close()
        print("Database connection closed")
        
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")

if __name__ == "__main__":
    main()