import os
import time
import json
import requests
import logging
import sqlite3
import traceback
import threading
import pandas as pd
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm
from openpyxl import Workbook
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Load .env config
load_dotenv(".env.local")

# Logging config
log_file = "canonical_checker.log"
logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Constants
SAVE_DIR = os.getcwd()
OUTPUT_FILE = os.path.join(SAVE_DIR, "canonical_mismatches.xlsx")
CACHE_DB = os.path.join(SAVE_DIR, "url_cache.sqlite")

# AWS S3
accessKeyId = os.getenv("AWS_ACCESS_KEY_ID")
secretAccessKey = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_REGION", "us-east-1")

S3_BUCKET = "jobtrees-media-assets"
S3_PUBLIC_PATH = "cannonical/"

# Sitemap URLs
sitemap_list = [
    "https://www.jobtrees.com/sitemap_page.xml",
    "https://www.jobtrees.com/sitemap_hierarchy.xml",
    "https://www.jobtrees.com/sitemap_article.xml",
    "https://www.jobtrees.com/sitemap_role.xml",
    "https://www.jobtrees.com/sitemap_tree.xml",
    "https://www.jobtrees.com/sitemap_video.xml",
    "https://www.jobtrees.com/sitemap_videoArticle.xml",
    "https://www.jobtrees.com/api/sitemap_pSEO/sitemap_index_pSEO.xml",
    "https://www.jobtrees.com/api/sitemap_city/sitemap_index_browse_city.xml",
    "https://www.jobtrees.com/api/sitemap_role/sitemap_index_browse_role.xml",
    "https://www.jobtrees.com/api/sitemap/sitemap_Alljobs.xml",
    "https://www.jobtrees.com/api/sitemap/jobtrees_postings_1.xml"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def create_retry_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

session = create_retry_session()
thread_local = threading.local()

def get_connection():
    if not hasattr(thread_local, "conn"):
        conn = sqlite3.connect(CACHE_DB, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS cache (url TEXT PRIMARY KEY, canonical TEXT, status INTEGER)")
        conn.commit()
        thread_local.conn = conn
    return thread_local.conn

def is_cached(url):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT canonical, status FROM cache WHERE url=?", (url,))
    return cursor.fetchone()

def update_cache(url, canonical, status):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("REPLACE INTO cache (url, canonical, status) VALUES (?, ?, ?)", (url, canonical, status))
    conn.commit()

def fetch_sitemap(url, visited=set()):
    if url in visited:
        return []
    visited.add(url)

    try:
        response = session.get(url, timeout=10)
    except Exception as e:
        logging.warning(f"Failed to fetch sitemap: {url} - {e}")
        return []

    if response.status_code != 200:
        return []

    urls = []
    try:
        root = ET.fromstring(response.text)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        for sitemap in root.findall(".//ns:loc", namespace):
            nested_url = sitemap.text
            if nested_url.endswith(".xml"):
                urls.extend(fetch_sitemap(nested_url, visited))
            else:
                urls.append(nested_url)
    except ET.ParseError:
        return []

    return urls

def fetch_canonical_url(page_url):
    cached = is_cached(page_url)
    if cached:
        return cached

    try:
        response = session.get(page_url, timeout=10)
        status_code = response.status_code
        logging.info(f"Checked: {page_url} | Status Code: {status_code}")

        if status_code != 200:
            update_cache(page_url, None, status_code)
            return (None, status_code)

        soup = BeautifulSoup(response.text, "html.parser")
        canonical_tag = soup.find("link", {"rel": "canonical"})
        canonical_url = canonical_tag["href"] if canonical_tag and canonical_tag.get("href") else None

        update_cache(page_url, canonical_url, status_code)
        return (canonical_url, status_code)

    except Exception as e:
        logging.error(f"Error fetching {page_url}: {e}")
        update_cache(page_url, None, -1)
        return (None, -1)

def check_canonical_mismatch(url_list, max_workers=30):
    mismatches = []

    def process_url(url):
        canonical_url, status_code = fetch_canonical_url(url)
        if canonical_url and canonical_url != url:
            logging.warning(f"Mismatch: {url} → {canonical_url}")
            return (url, canonical_url, status_code)
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_url, url): url for url in url_list}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Checking URLs"):
            result = future.result()
            if result:
                mismatches.append(result)

    return mismatches

def upload_to_s3():
    try:
        if accessKeyId and secretAccessKey:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=accessKeyId,
                aws_secret_access_key=secretAccessKey,
                region_name=region,
            )
        else:
            s3 = boto3.client("s3")  # IAM role fallback

        upload_path = f"{S3_PUBLIC_PATH}canonical_mismatches.xlsx"
        print(f"🔍 Uploading to: s3://{S3_BUCKET}/{upload_path}")
        s3.upload_file(OUTPUT_FILE, S3_BUCKET, upload_path)
        print(f"🚀 Uploaded to s3://{S3_BUCKET}/{upload_path}")
        logging.info("Upload successful")

    except Exception as e:
        print("❌ Failed to upload to S3.")
        traceback.print_exc()
        logging.error("S3 upload failed: %s", str(e))

if __name__ == "__main__":
    global_start = time.time()
    logging.info("=== Starting Canonical Checker Script ===")
    all_mismatches = {}

    for i, sitemap_url in enumerate(sitemap_list, 1):
        print(f"\n🚀 Processing Sitemap: {sitemap_url}")
        logging.info(f"Processing Sitemap: {sitemap_url}")
        start = time.time()
        sitemap_urls = list(set(fetch_sitemap(sitemap_url)))
        logging.info(f"Fetched {len(sitemap_urls)} URLs from sitemap")

        if sitemap_urls:
            mismatches = check_canonical_mismatch(sitemap_urls)
            logging.info(f"{len(mismatches)} mismatches found")
            if mismatches:
                name_part = sitemap_url.split("/")[-1].replace(".xml", "").replace("sitemap_", "")
                sheet_name = f"{i}_{name_part[:25]}"
                all_mismatches[sheet_name] = mismatches

        elapsed = round(time.time() - start, 2)
        print(f"✅ Finished {sitemap_url} in {elapsed}s")
        logging.info(f"Finished {sitemap_url} in {elapsed}s")

    os.makedirs(SAVE_DIR, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        if all_mismatches:
            for sheet_name, mismatches in all_mismatches.items():
                df = pd.DataFrame(mismatches, columns=["Sitemap URL", "Canonical URL", "Status Code"])
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            df = pd.DataFrame(columns=["Sitemap URL", "Canonical URL", "Status Code"])
            df.to_excel(writer, index=False, sheet_name="No Data")

    print(f"📁 Excel file saved to: {OUTPUT_FILE}")
    logging.info(f"Excel file saved to: {OUTPUT_FILE}")

    upload_to_s3()

    total_time = round(time.time() - global_start, 2)
    print("🧠 Finished. Closing DB connection and saving logs.")
    logging.info("Script completed in %.2fs", total_time)
    logging.info("=============================\n\n")
