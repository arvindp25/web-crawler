import requests
import json
import csv
import re
import time
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import io

class CommonCrawlExtractor:
    def __init__(self):
        self.cc_index_server = "https://index.commoncrawl.org"
        self.cc_crawl = "CC-MAIN-2025-13"  # March 2025 crawl
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; CommonCrawlExtractor/1.0)'
        })
        
        # Industry keywords for classification
        self.industry_keywords = {
            'technology': ['software', 'tech', 'IT', 'digital', 'app', 'web development'],
            'healthcare': ['medical', 'health', 'hospital', 'clinic', 'pharmaceutical'],
            'finance': ['bank', 'financial', 'insurance', 'investment', 'accounting'],
            'retail': ['shop', 'store', 'retail', 'ecommerce', 'fashion'],
            'construction': ['construction', 'building', 'contractor', 'architecture'],
            'education': ['school', 'university', 'education', 'training', 'college'],
            'manufacturing': ['manufacturing', 'factory', 'production', 'industrial'],
            'consulting': ['consulting', 'advisory', 'professional services'],
            'real_estate': ['real estate', 'property', 'realty', 'housing'],
            'hospitality': ['hotel', 'restaurant', 'tourism', 'hospitality']
        }

    def search_australian_domains(self, limit=10000):
        """Search for Australian domains in Common Crawl index"""
        print("Searching for Australian domains...")
        
        # Search for .com.au and .au domains
        domains = ['.com.au/*', '.au/*']
        all_results = []
        
        for domain_pattern in domains:
            url = f"{self.cc_index_server}/{self.cc_crawl}-index"
            params = {
                'url': domain_pattern,
                'output': 'json',
                'limit': limit // len(domains)
            }
            
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                # Parse JSONL response
                for line in response.text.strip().split('\n'):
                    if line:
                        try:
                            result = json.loads(line)
                            all_results.append(result)
                        except json.JSONDecodeError:
                            continue
                            
            except requests.RequestException as e:
                print(f"Error searching for {domain_pattern}: {e}")
                continue
                
            time.sleep(0.1)  # Rate limiting
        
        print(f"Found {len(all_results)} Australian URLs")
        return all_results

    def filter_company_pages(self, results):
        """Filter for pages likely to contain company information"""
        company_indicators = [
            'about', 'contact', 'company', 'business', 'services',
            'home', 'index', 'main', 'www'
        ]
        
        filtered = []
        for result in results:
            url = result.get('url', '').lower()
            
            # Skip obvious non-company pages
            skip_patterns = [
                'blog', 'news', 'article', 'product', 'category',
                'search', 'login', 'register', 'cart', 'checkout'
            ]
            
            if any(pattern in url for pattern in skip_patterns):
                continue
                
            # Prioritize company-related pages
            if any(indicator in url for indicator in company_indicators):
                result['priority'] = 'high'
            else:
                result['priority'] = 'normal'
                
            filtered.append(result)
        
        return filtered

    def fetch_and_parse_content(self, cc_result):
        """Fetch content from Common Crawl and extract company info"""
        try:
            # Build the archive URL using CloudFront CDN
            filename = cc_result['filename']
            offset = cc_result['offset']
            length = cc_result['length']
            
            # Try CloudFront first (recommended), fallback to S3
            archive_urls = [
                f"https://data.commoncrawl.org/{filename}",  # CloudFront CDN
                f"https://commoncrawl.s3.amazonaws.com/{filename}"  # Direct S3 (fallback)
            ]
            
            content = None
            for archive_url in archive_urls:
                try:
                    # Fetch the specific record with retry logic
                    headers = {'Range': f'bytes={offset}-{int(offset) + int(length) - 1}'}
                    response = self.session.get(archive_url, headers=headers, timeout=60)
                    
                    if response.status_code == 206:  # Partial content success
                        content = self.parse_warc_record(response.content)
                        if content:
                            break
                    elif response.status_code == 403:
                        print(f"403 Forbidden for {archive_url}, trying next...")
                        continue
                    else:
                        response.raise_for_status()
                        
                except requests.RequestException as e:
                    print(f"Request failed for {archive_url}: {e}")
                    continue
            
            if not content:
                return None
            
            # Extract company information
            return self.extract_company_info(cc_result['url'], content)
            
        except Exception as e:
            print(f"Error processing {cc_result.get('url', 'unknown')}: {e}")
            return None

    def parse_warc_record(self, warc_data):
        """Parse WARC record to extract HTML content"""
        try:
            # WARC records are separated by double newlines
            parts = warc_data.split(b'\r\n\r\n', 2)
            if len(parts) < 3:
                return None
            
            # The HTML content is in the third part
            html_content = parts[2]
            
            # Handle gzip compression
            if html_content.startswith(b'\x1f\x8b'):
                html_content = gzip.decompress(html_content)
            
            return html_content.decode('utf-8', errors='ignore')
            
        except Exception:
            return None

    def extract_company_info(self, url, html_content):
        """Extract company information from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract company name
            company_name = self.extract_company_name(soup)
            
            # Extract industry
            industry = self.classify_industry(html_content, company_name)
            
            # Additional metadata
            title = soup.find('title')
            title_text = title.get_text().strip() if title else ''
            
            description = soup.find('meta', attrs={'name': 'description'})
            description_text = description.get('content', '').strip() if description else ''
            
            return {
                'url': url,
                'company_name': company_name,
                'industry': industry,
                'title': title_text,
                'description': description_text,
                'domain': urlparse(url).netloc
            }
            
        except Exception as e:
            print(f"Error extracting info from {url}: {e}")
            return None

    def extract_company_name(self, soup):
        """Extract company name from various HTML elements"""
        # Try different methods to find company name
        methods = [
            # Schema.org structured data
            lambda: soup.find('span', {'itemprop': 'name'}),
            lambda: soup.find('div', {'itemprop': 'name'}),
            
            # Common header patterns
            lambda: soup.find('h1'),
            lambda: soup.find('h2'),
            
            # Logo alt text
            lambda: soup.find('img', {'alt': re.compile(r'logo', re.I)}),
            
            # Navigation brand
            lambda: soup.find('a', {'class': re.compile(r'brand|logo', re.I)}),
            
            # Title tag
            lambda: soup.find('title')
        ]
        
        for method in methods:
            try:
                element = method()
                if element:
                    text = element.get_text().strip()
                    if text and len(text) < 100:  # Reasonable company name length
                        # Clean up the text
                        text = re.sub(r'\s+', ' ', text)
                        text = text.split('|')[0].split('-')[0].strip()
                        return text
            except:
                continue
        
        return "Unknown"

    def classify_industry(self, html_content, company_name):
        """Classify industry based on content analysis"""
        text = (html_content + ' ' + company_name).lower()
        
        industry_scores = {}
        for industry, keywords in self.industry_keywords.items():
            score = sum(text.count(keyword.lower()) for keyword in keywords)
            if score > 0:
                industry_scores[industry] = score
        
        if industry_scores:
            return max(industry_scores, key=industry_scores.get)
        
        return "Other"

    def process_batch(self, results, batch_size=20, max_workers=3):
        """Process results in batches with threading and better rate limiting"""
        companies = []
        
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(results)-1)//batch_size + 1}")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.fetch_and_parse_content, result): result 
                          for result in batch}
                
                for future in as_completed(futures):
                    try:
                        company_info = future.result()
                        if company_info and company_info['company_name'] != "Unknown":
                            companies.append(company_info)
                            print(f"✓ Found: {company_info['company_name']} ({company_info['domain']})")
                    except Exception as e:
                        print(f"Batch processing error: {e}")
            
            # Longer rate limiting between batches to avoid 403/503 errors
            time.sleep(2)
        
        return companies

    def save_results(self, companies, filename='australian_companies.csv'):
        """Save extracted company data to CSV"""
        if not companies:
            print("No company data to save")
            return
        
        df = pd.DataFrame(companies)
        
        # Remove duplicates based on domain
        df = df.drop_duplicates(subset=['domain'])
        
        # Sort by company name
        df = df.sort_values('company_name')
        
        df.to_csv(filename, index=False)
        print(f"Saved {len(df)} companies to {filename}")
        
        # Print summary
        print("\nIndustry distribution:")
        print(df['industry'].value_counts())

def main():
    """Main execution function"""
    extractor = CommonCrawlExtractor()
    
    # Step 1: Search for Australian domains
    print("Starting Australian company extraction from Common Crawl...")
    results = extractor.search_australian_domains(limit=2000)  # Reduced for better success rate
    
    if not results:
        print("No results found. Check your connection and try again.")
        return
    
    # Step 2: Filter for company pages
    filtered_results = extractor.filter_company_pages(results)
    print(f"Filtered to {len(filtered_results)} potential company pages")
    
    # Step 3: Process and extract company information (smaller batch for testing)
    companies = extractor.process_batch(filtered_results[:200])  # Start smaller
    
    # Step 4: Save results
    extractor.save_results(companies)
    
    print(f"\nExtraction complete! Found {len(companies)} companies.")
    if companies:
        print("Sample companies found:")
        for company in companies[:5]:
            print(f"  - {company['company_name']} ({company['industry']}) - {company['domain']}")

if __name__ == "__main__":
    main()