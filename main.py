import requests
import gzip
import json
import io
import pandas as pd
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class CommonCrawlExtractor:
    
    def __init__(self, index='CC-MAIN-2025-13'):
        self.index = index
        self.index_url = f"https://index.commoncrawl.org/{index}-index"
        # Improved regex pattern for Australian domains
        self.au_domain_pattern = re.compile(r'^https?://([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+com\.au|net\.au|org\.au|edu\.au|gov\.au|asn\.au|id\.au|info\.au|conf\.au|oz\.au|act\.au|nsw\.au|nt\.au|qld\.au|sa\.au|tas\.au|vic\.au|wa\.au(/.*)?$')
        # Keywords associated with business websites
        self.business_keywords = ['about', 'contact', 'services', 'products', 'team', 'company', 'business']
        # Setup session with retries
        self.session = self._setup_session()
        
    def _setup_session(self):
        """Set up a session with retry capability"""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
        
    def get_index_urls(self, limit=None):
        """Get list of index file URLs from CommonCrawl"""
        try:
            response = self.session.get(f"{self.index_url}?output=json")
            response.raise_for_status()
            index_files = response.json()
            
            if limit:
                return [item['url'] for item in index_files[:limit]]
            return [item['url'] for item in index_files]
        except Exception as e:
            print(f"Error fetching index URLs: {str(e)}")
            return []
            
    def extract_au_websites(self, index_url):
        """Extract Australian websites from a single index file"""
        au_results = []
        
        try:
            # Add delay to avoid rate limiting
            time.sleep(random.uniform(0.5, 2.0))
            
            response = self.session.get(index_url, stream=True)
            response.raise_for_status()
            
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        url = record.get('url', '')
                        
                        # Skip if not an Australian domain
                        if not self.au_domain_pattern.match(url):
                            continue
                            
                        # Extract domain and parse URL
                        parsed_url = urlparse(url)
                        domain = parsed_url.netloc
                        path = parsed_url.path
                        
                        # Extract MIME type and status
                        mime_type = record.get('mime', '')
                        status = record.get('status', '')
                        
                        # Skip non-HTML content or error pages
                        if 'text/html' not in mime_type or status != '200':
                            continue
                            
                        # Check if this is likely a business page by examining the path
                        has_business_indicator = any(keyword in path.lower() for keyword in self.business_keywords)
                        
                        # Initialize metadata
                        metadata = {
                            'company_name': '',
                            'industry': '',
                            'description': ''
                        }
                        
                        # Extract metadata if available
                        if 'metadata' in record:
                            meta = record['metadata']
                            for key in metadata:
                                if key in meta:
                                    metadata[key] = meta[key]
                                    
                        # Try to extract company name from domain if not in metadata
                        if not metadata['company_name']:
                            # Extract the second-level domain as company name (example.com.au -> example)
                            domain_parts = domain.split('.')
                            if len(domain_parts) >= 3:
                                metadata['company_name'] = domain_parts[-3].capitalize()
                            else:
                                metadata['company_name'] = domain_parts[0].capitalize()
                        
                        # Extract industry from URL patterns if not in metadata
                        if not metadata['industry']:
                            domain_tld = '.'.join(domain.split('.')[-2:])  # Get TLD (com.au, org.au)
                            if 'edu.au' in domain_tld:
                                metadata['industry'] = 'Education'
                            elif 'gov.au' in domain_tld:
                                metadata['industry'] = 'Government'
                            elif 'org.au' in domain_tld:
                                metadata['industry'] = 'Non-profit'
                            elif 'com.au' in domain_tld:
                                metadata['industry'] = 'Commercial'
                            else:
                                metadata['industry'] = 'Other'
                                
                        # Create result dictionary
                        result = {
                            'url': url,
                            'domain': domain,
                            'company_name': metadata['company_name'],
                            'industry': metadata['industry'],
                            'description': metadata.get('description', ''),
                            'path': path,
                            'is_business_page': has_business_indicator,
                            'crawl_time': record.get('timestamp', '')
                        }
                        
                        au_results.append(result)
                    except json.JSONDecodeError as E:
                        print(E)
                        continue
                    except Exception as e:
                        # Skip individual record errors
                        continue
                        
        except Exception as e:
            print(f"Error processing {index_url}: {str(e)}")
            
        return au_results
        
    def extract_websites(self, min_count=200000, max_workers=10, limit_index_files=None):
        """Extract Australian websites using multiple threads"""
        index_urls = self.get_index_urls(limit=limit_index_files)
        results = []
        
        if not index_urls:
            print("No index URLs found!")
            return pd.DataFrame()
            
        print(f"Processing {len(index_urls)} index files...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for url in index_urls:
                futures.append(executor.submit(self.extract_au_websites, url))
                
            for future in tqdm(futures, desc="Processing index files"):
                batch_results = future.result()
                results.extend(batch_results)
                
                # Provide progress updates
                if len(results) % 10000 == 0:
                    print(f"Extracted {len(results)} websites so far...")
                
                if len(results) >= min_count:
                    print(f"Reached minimum website count: {len(results)}")
                    break
                    
        if len(results) < min_count:
            print(f"Warning: Only extracted {len(results)} websites, below minimum of {min_count}")
            
        # Convert to DataFrame and remove duplicates
        df = pd.DataFrame(results)
        if not df.empty:
            # Remove duplicates by domain, keeping the most likely business page
            df = df.sort_values('is_business_page', ascending=False)
            df = df.drop_duplicates(subset=['domain'], keep='first')
            
            # Add some basic statistics
            print(f"Industry distribution:\n{df['industry'].value_counts()}")
            
        return df
        
    def save_to_csv(self, df, output_path='australian_websites.csv'):
        """Save extracted websites to CSV"""
        if df.empty:
            print("No data to save!")
            return
            
        df.to_csv(output_path, index=False)
        print(f"Saved {len(df)} Australian websites to {output_path}")
        
    def enrich_with_whois(self, df, sample_size=1000):
        """Enrich data with WHOIS information for a sample of domains"""
        try:
            import whois
        except ImportError:
            print("whois package not installed. Run 'pip install python-whois' to use this feature.")
            return df
            
        if df.empty:
            return df
            
        # Take a sample if the dataframe is large
        sample_df = df.sample(min(sample_size, len(df))) if len(df) > sample_size else df
        
        print(f"Enriching {len(sample_df)} domains with WHOIS data...")
        
        whois_data = []
        for _, row in tqdm(sample_df.iterrows(), total=len(sample_df)):
            domain = row['domain']
            try:
                # Add delay to avoid rate limiting
                time.sleep(random.uniform(1.0, 3.0))
                
                w = whois.whois(domain)
                whois_data.append({
                    'domain': domain,
                    'registrar': w.registrar,
                    'creation_date': w.creation_date,
                    'expiration_date': w.expiration_date,
                    'last_updated': w.updated_date
                })
            except Exception as e:
                continue
                
        whois_df = pd.DataFrame(whois_data)
        
        # Merge whois data with original dataframe
        if not whois_df.empty:
            merged_df = pd.merge(df, whois_df, on='domain', how='left')
            return merged_df
        
        return df

if __name__ == "__main__":
    # Create extractor instance
    extractor = CommonCrawlExtractor(index='CC-MAIN-2025-03')
    
    # Extract websites with reasonable defaults
    websites_df = extractor.extract_websites(
        min_count=200,  # Target number of websites
        max_workers=4,    # Number of parallel threads
        limit_index_files=10  # Limit number of index files to process (for testing)
    )
    
    
    # Save results
    extractor.save_to_csv(websites_df, 'australian_websites_2025.csv')
    
    # Show summary statistics
    if not websites_df.empty:
        print("\nWebsite statistics:")
        print(f"Total websites: {len(websites_df)}")
        print(f"Top industries:\n{websites_df['industry'].value_counts().head(10)}")
        print(f"Top level domains:\n{websites_df['domain'].apply(lambda x: '.'.join(x.split('.')[-2:])).value_counts().head(5)}")