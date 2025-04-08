import time
import pandas as pd
from lxml import etree
import os
import gzip
import zipfile
from tqdm import tqdm
import requests
import io

class ABRExtractor:
    """Extract business data from Australian Business Register XML files"""
    
    def __init__(self, data_dir='./abr_data'):
        """Initialize with directory for downloaded ABR data"""
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
    
    def download_abr_data(self, url=None):
        """Download a single ABR bulk extract file
        
        If URL is not provided, uses default data.gov.au URL for ABR data
        """
        if url is None:
            url = "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_20.zip"

        print(f"Downloading ABR data from {url}")
        
        # Create a unique filename based on the URL
        filename = os.path.basename(url)
        if not filename.endswith('.zip'):
            filename = f"abr_data_{hash(url) % 10000}.zip"
            
        local_path = os.path.join(self.data_dir, filename)
        response = requests.get(url, stream=True)
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(local_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading ABR data") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        print(f"Downloaded ABR data to {local_path}")
        
        # Extract XML files and return their paths
        xml_files = []
        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            extract_dir = os.path.join(self.data_dir, os.path.splitext(os.path.basename(local_path))[0])
            os.makedirs(extract_dir, exist_ok=True)
            
            for file in zip_ref.namelist():
                if file.endswith('.xml'):
                    zip_ref.extract(file, extract_dir)
                    xml_files.append(os.path.join(extract_dir, file))
                    print(f"Extracted {file}")
        
        return xml_files
    
    def parse_xml_file(self, file_path):
        """Parse ABR XML file and extract business information"""
        print(f"Parsing ABR XML file: {file_path}")
    
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
    
        # Determine file type based on extension
        file_opener = None
        if file_path.endswith('.gz'):
            file_opener = gzip.open
        elif file_path.endswith('.zip'):
            raise ValueError("ZIP files should be extracted before parsing")
        else:
            file_opener = open
    
        businesses = []
    
        try:
            with file_opener(file_path, "rb") as f:
                # Create iterparse context for memory-efficient parsing
                context = etree.iterparse(f, events=('end',), tag="{*}ABR")
                
                with tqdm(desc=f"Parsing {os.path.basename(file_path)}") as pbar:
                    for _, elem in context:
                        try:
                            # Extract ABN (value and status)
                            abn_elem = elem.find(".//ABN")
                            if abn_elem is None:
                                continue
                                
                            abn = abn_elem.text
                            status = abn_elem.get('status')
                            start_date = abn_elem.get('ABNStatusFromDate')
                            
                            # Extract entity type
                            entity_type_ind_elem = elem.find(".//EntityTypeInd")
                            entity_type_ind = entity_type_ind_elem.text if entity_type_ind_elem is not None else None
                            
                            entity_type_text_elem = elem.find(".//EntityTypeText")
                            entity_type_text = entity_type_text_elem.text if entity_type_text_elem is not None else None
                            
                            # Extract entity name
                            name_elem = elem.find(".//NonIndividualNameText")
                            entity_name = name_elem.text if name_elem is not None else None
                            
                            # Extract address
                            address_details = elem.find(".//AddressDetails")
                            state = postcode = None
                            
                            if address_details is not None:
                                state_elem = address_details.find("./State")
                                state = state_elem.text if state_elem is not None else None
                                
                                postcode_elem = address_details.find("./Postcode")
                                postcode = postcode_elem.text if postcode_elem is not None else None
                            
                            # Extract ASIC number if available
                            asic_elem = elem.find(".//ASICNumber")
                            asic_number = asic_elem.text if asic_elem is not None else None
                            asic_type = asic_elem.get('ASICNumberType') if asic_elem is not None else None
                            
                            # Extract GST status if available
                            gst_elem = elem.find(".//GST")
                            gst_status = gst_elem.get('status') if gst_elem is not None else None
                            gst_from_date = gst_elem.get('GSTStatusFromDate') if gst_elem is not None else None
                            
                            # Extract trading names if available
                            trading_names = []
                            other_entities = elem.findall(".//OtherEntity")
                            for other_entity in other_entities:
                                name_elem = other_entity.find(".//NonIndividualNameText")
                                if name_elem is not None:
                                    trading_names.append(name_elem.text)
                            
                            # Compile business record
                            business = {
                                'abn': abn,
                                'entity_name': entity_name,
                                'entity_type_code': entity_type_ind,
                                'entity_type': entity_type_text,
                                'entity_status': status,
                                'entity_postcode': postcode,
                                'entity_state': state,
                                'entity_start_date': start_date,
                                'asic_number': asic_number,
                                'asic_type': asic_type,
                                'gst_status': gst_status,
                                'gst_from_date': gst_from_date,
                                'trading_names': '; '.join(trading_names) if trading_names else None
                            }
                            
                            businesses.append(business)
                            pbar.update(1)
                            
                            # Clear element to free memory
                            elem.clear()
                            
                            # Also eliminate now-empty references from the root
                            # to avoid memory leaks
                            while elem.getprevious() is not None:
                                del elem.getparent()[0]
                                
                        except Exception as e:
                            print(f"Error parsing entity: {str(e)}")
                
        except Exception as e:
            raise Exception(f"Error parsing XML file {file_path}: {str(e)}")
        
        # Convert to DataFrame
        df = pd.DataFrame(businesses)
        print(f"Extracted {len(df)} businesses from {file_path}")
        return df
    
    def process_multiple_urls(self, urls, output_path='combined_abr_businesses.csv'):
        """Download, parse and save ABR data from multiple URLs
        
        Args:
            urls: List of URLs to download and process
            output_path: Path to save the combined CSV file
        """
        all_dataframes = []
        
        for url in urls:
            try:
                # Download data from this URL
                xml_files = self.download_abr_data(url)
                
                # Process each XML file
                for xml_file in xml_files:
                    df = self.parse_xml_file(xml_file)
                    all_dataframes.append(df)
                    print(f"Processed {xml_file}, got {len(df)} records")
            except Exception as e:
                print(f"Error processing URL {url}: {str(e)}")
        
        # Combine all data
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Save to CSV
            combined_df.to_csv(output_path, index=False)
            print(f"Saved {len(combined_df)} combined ABR businesses to {output_path}")
            
            return combined_df
        else:
            print("No data was extracted")
            return pd.DataFrame()
    
    def extract_and_save(self, output_path='abr_businesses.csv'):
        """Process existing XML files in the data directory
        For backward compatibility with the original code
        """
        try:
            xml_files = []
            for root, _, files in os.walk(self.data_dir):
                for file in files:
                    if file.endswith('.xml'):
                        xml_files.append(os.path.join(root, file))
            
            if not xml_files:
                xml_files = self.download_abr_data()
            
            # Parse each file and collect DataFrames
            all_dfs = []
            
            for file_path in xml_files:
                businesses_df = self.parse_xml_file(file_path)
                all_dfs.append(businesses_df)
            
            # Combine all data
            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                
                # Save to CSV
                combined_df.to_csv(output_path, index=False)
                print(f"Saved {len(combined_df)} ABR businesses to {output_path}")
                
                return combined_df
            else:
                print("No data was extracted from any files")
                return pd.DataFrame()
            
        except Exception as e:
            print(f"Error in extract_and_save: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    # Define both URLs
    urls = [
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_20.zip",
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/c2dd888e-5c43-4b92-a9c9-b67462ed17a0/download/public_split_21_40.zip"
    ]
    
    extractor = ABRExtractor()
    combined_data = extractor.process_multiple_urls(urls, 'combined_abr_businesses.csv')
    print(f"Successfully processed {len(combined_data)} businesses from all sources")