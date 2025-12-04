#!/usr/bin/env python3
"""
SRI LANKA VEHICLE MASTER SCRAPER (Riyasewana + Patpat + Ikman)
- COMBINED: Scrapes all 3 sites sequentially.
- OUTPUT: 
    1. Unified "Basic" CSV (All sites combined).
    2. Individual "Detailed" CSVs for each site.
- ARCHITECTURE: Class-based isolation to prevent variable conflicts.
"""

import time
import requests
import random
import csv
import re
import threading
import queue
import os
import cloudscraper
import sqlite3
import logging
import psutil
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ==========================================
# ⚙️ GLOBAL CONFIGURATION
# ==========================================
FLARESOLVERR_URL = "http://localhost:8191/v1"
DAYS_TO_KEEP = 15
BATCH_SIZE = 50
DATA_FOLDER = "vehicle_data_master"

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

# Timestamp for files
TS = time.strftime('%Y-%m-%d_%H-%M')

# ==========================================
# 🛠️ SHARED TOOLS
# ==========================================

class UnifiedBatchWriter:
    """Writes to the Master Basic CSV and individual files."""
    def __init__(self, filename, fieldnames):
        self.filepath = filename
        self.fieldnames = fieldnames
        self.buffer = []
        self.lock = threading.Lock()
        
        # Initialize file with headers
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def add_row(self, row):
        with self.lock:
            # Ensure row only has keys that exist in fieldnames
            clean_row = {k: row.get(k, '') for k in self.fieldnames}
            self.buffer.append(clean_row)
            if len(self.buffer) >= BATCH_SIZE:
                self.flush()

    def flush(self):
        with self.lock:
            if not self.buffer: return
            try:
                with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writerows(self.buffer)
                self.buffer = []
            except Exception as e:
                print(f"Write Error: {e}")

class FlareSolverrClient:
    """Shared Client for Cloudflare Bypassing"""
    def __init__(self):
        self.url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        # Windows Chrome Emulation
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        self.session_id = None

    def create_session(self, prefix="sess"):
        try:
            sid = f"{prefix}_{random.randint(1000,9999)}_{int(time.time())}"
            payload = {"cmd": "sessions.create", "session": sid}
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=10)
            if r.status_code == 200:
                self.session_id = r.json().get("session")
                return True
        except: pass
        return False

    def destroy_session(self):
        if self.session_id:
            try:
                requests.post(self.url, json={"cmd": "sessions.destroy", "session": self.session_id}, headers=self.headers, timeout=5)
            except: pass

    def fetch(self, url, method="hybrid"):
        # Hybrid: Try CloudScraper first, then FlareSolverr
        if method == "hybrid":
            try:
                time.sleep(random.uniform(1.0, 3.0)) # Anti-ban jitter
                r = self.scraper.get(url, timeout=20)
                if r.status_code == 200 and len(r.text) > 1000:
                    if "Attention Required!" not in r.text and "Access denied" not in r.text:
                        return r.text
            except: pass
        
        # Fallback to FlareSolverr
        if not self.session_id: self.create_session()
        payload = {"cmd": "request.get", "url": url, "maxTimeout": 60000}
        if self.session_id: payload["session"] = self.session_id
        try:
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=65)
            if r.status_code == 200:
                resp = r.json()
                if resp.get("status") == "ok":
                    return resp.get("solution", {}).get("response", "")
        except: pass
        return None

# ==========================================
# 1️⃣ RIYASEWANA SCRAPER CLASS
# ==========================================
class RiyasewanaScraper:
    def __init__(self, unified_writer):
        self.unified_writer = unified_writer
        self.detail_writer = None
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.stats = {'saved': 0, 'dupes': 0}
        self.lock = threading.Lock()
        self.seen_urls = set()
        
        # Config
        self.makes = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 'daihatsu', 'kia', 'hyundai', 'audi', 'bmw', 'mercedes-benz', 'land-rover', 'tata', 'mahindra']
        self.types = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']
        self.max_pages = 160
        self.workers_search = 3
        self.workers_detail = 5

    def run(self):
        print("\n" + "="*50)
        print("🚀 STARTING: RIYASEWANA SCRAPER")
        print("="*50)
        
        cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
        det_csv = f"{DATA_FOLDER}/RIYASEWANA_DETAILED_{TS}.csv"
        
        # Headers
        det_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL']
        self.detail_writer = UnifiedBatchWriter(det_csv, det_fields)
        
        client = FlareSolverrClient()
        
        # Start Extractors
        ex_pool = ThreadPoolExecutor(max_workers=self.workers_detail)
        for _ in range(self.workers_detail):
            ex_pool.submit(self.extractor_worker, cutoff)
            
        # Start Crawling
        tasks = []
        for m in self.makes:
            for t in self.types:
                for p in range(1, self.max_pages + 1):
                    tasks.append((m, t, p))
        random.shuffle(tasks)
        
        with ThreadPoolExecutor(max_workers=self.workers_search) as s_pool:
            futures = [s_pool.submit(self.harvest_task, client, m, t, p, cutoff) for m, t, p in tasks]
            with tqdm(total=len(futures), desc="Riya Crawl") as pbar:
                for _ in as_completed(futures):
                    pbar.update(1)
        
        self.queue.join()
        self.stop_event.set()
        ex_pool.shutdown(wait=True)
        self.detail_writer.flush()
        print(f"✅ Riyasewana Done. Saved: {self.stats['saved']}")

    def harvest_task(self, client, make, v_type, page, cutoff):
        url = f"https://riyasewana.com/search/{v_type}/{make}"
        if page > 1: url += f"?page={page}"
        
        html = client.fetch(url)
        if not html: return
        
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/buy/' in href and '-sale-' in href:
                with self.lock:
                    if href in self.seen_urls:
                        self.stats['dupes'] += 1
                        continue
                    self.seen_urls.add(href)
                
                title = link.get_text(" ", strip=True)
                if len(title) < 5: 
                    h2 = link.find_parent('h2')
                    if h2: title = h2.get_text(" ", strip=True)
                
                # Basic Date Check
                date_val = "Check_Page"
                price = "0"
                container = link.find_parent('li') or link.find_parent('div', class_=re.compile('item'))
                if container:
                    txt = container.get_text(" ", strip=True)
                    dm = re.search(r'(\d{4}-\d{2}-\d{2})', txt)
                    if dm:
                        try:
                            if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff: continue
                            date_val = dm.group(1)
                        except: pass
                    pm = re.search(r'Rs\.?\s*([\d,]+)', txt)
                    if pm: price = pm.group(1).replace(',', '')

                self.queue.put({'url': href, 'date': date_val, 'make': make, 'type': v_type, 'title': title, 'price': price})

    def extractor_worker(self, cutoff):
        client = FlareSolverrClient()
        re_yom = re.compile(r'\b(20\d{2}|19\d{2})\b')
        re_phone = re.compile(r'(?:07\d|0\d{2})[- ]?\d{3}[- ]?\d{4}')

        while not self.stop_event.is_set() or not self.queue.empty():
            try: item = self.queue.get(timeout=3)
            except: continue
            
            try:
                html = client.fetch(item['url'])
                if not html: continue
                soup = BeautifulSoup(html, "html.parser")
                full_text = soup.get_text(" ", strip=True)

                # Date Logic
                if item['date'] == "Check_Page":
                    dm = re.search(r'(\d{4}-\d{2}-\d{2})', full_text)
                    if dm:
                        try:
                            if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff: continue
                            item['date'] = dm.group(1)
                        except: pass
                    else: item['date'] = datetime.now().strftime("%Y-%m-%d")

                details = {'YOM': '', 'Transmission': '', 'Fuel': '', 'Engine': '', 'Mileage': '', 'Location': '', 'Contact': ''}
                
                # DOM Logic
                for label in soup.find_all('p', class_='moreh'):
                    txt = label.get_text(strip=True).lower()
                    parent = label.find_parent('td')
                    if parent:
                        val_td = parent.find_next_sibling('td')
                        if val_td:
                            val = val_td.get_text(strip=True)
                            if 'mileage' in txt: details['Mileage'] = val
                            elif 'engine' in txt: details['Engine'] = val
                            elif 'transmission' in txt: details['Transmission'] = val
                            elif 'fuel' in txt: details['Fuel'] = val
                            elif 'yom' in txt: details['YOM'] = val
                
                if not details['YOM']:
                    ym = re_yom.search(item['title'])
                    if ym: details['YOM'] = ym.group(1)
                
                phones = re_phone.findall(full_text)
                if phones: details['Contact'] = " / ".join(set([p.replace('-','').replace(' ','') for p in phones]))

                # Write to Basic Unified
                basic_row = {
                    'Source': 'Riyasewana', 'Date': item['date'], 'Make': item['make'], 
                    'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 
                    'Price': item['price'], 'URL': item['url']
                }
                self.unified_writer.add_row(basic_row)

                # Write to Detailed
                detail_row = {**basic_row, **details}
                del detail_row['Source'] # Remove source from detailed if not needed
                self.detail_writer.add_row(detail_row)
                
                with self.lock: self.stats['saved'] += 1

            except: pass
            finally: self.queue.task_done()
        client.destroy_session()


# ==========================================
# 2️⃣ PATPAT SCRAPER CLASS
# ==========================================
class PatpatScraper:
    def __init__(self, unified_writer):
        self.unified_writer = unified_writer
        self.detail_writer = None
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.stats = {'saved': 0, 'dupes': 0}
        self.lock = threading.Lock()
        
        # Use SQLite for Patpat as per original code (it helps with larger datasets)
        self.db_path = f"{DATA_FOLDER}/patpat_seen.sqlite"
        self._init_db()
        
        self.makes = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 'kia', 'hyundai', 'micro', 'audi', 'bmw', 'tata']
        self.types = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']
        self.max_pages = 160
        self.workers = 5 # Lower workers for Patpat as it's heavy

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("CREATE TABLE IF NOT EXISTS seen (url TEXT PRIMARY KEY)")
        self.conn.commit()

    def is_seen(self, url):
        cur = self.conn.execute("SELECT 1 FROM seen WHERE url = ?", (url,))
        return cur.fetchone() is not None

    def mark_seen(self, url):
        try:
            self.conn.execute("INSERT OR IGNORE INTO seen (url) VALUES (?)", (url,))
            self.conn.commit()
        except: pass

    def run(self):
        print("\n" + "="*50)
        print("🚀 STARTING: PATPAT.LK SCRAPER")
        print("="*50)
        
        cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
        det_csv = f"{DATA_FOLDER}/PATPAT_DETAILED_{TS}.csv"
        det_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL', 'Description']
        self.detail_writer = UnifiedBatchWriter(det_csv, det_fields)
        
        # Start Extractor Threads
        ex_pool = ThreadPoolExecutor(max_workers=self.workers)
        for _ in range(self.workers):
            ex_pool.submit(self.extractor_worker, cutoff)
            
        # Start Crawling
        client = FlareSolverrClient()
        tasks = []
        for m in self.makes:
            for t in self.types:
                for p in range(1, self.max_pages + 1):
                    tasks.append((m, t, p))
        random.shuffle(tasks)
        
        with ThreadPoolExecutor(max_workers=self.workers) as s_pool:
            futures = [s_pool.submit(self.harvest_task, client, m, t, p, cutoff) for m, t, p in tasks]
            with tqdm(total=len(futures), desc="Patpat Crawl") as pbar:
                for _ in as_completed(futures):
                    pbar.update(1)
        
        self.queue.join()
        self.stop_event.set()
        ex_pool.shutdown(wait=True)
        self.detail_writer.flush()
        self.conn.close()
        print(f"✅ Patpat Done. Saved: {self.stats['saved']}")

    def harvest_task(self, client, make, v_type, page, cutoff):
        url = f"https://patpat.lk/search/{v_type}/{make}?page={page}"
        html = client.fetch(url, method="flaresolverr") # Patpat needs strict FlareSolverr
        if not html: return

        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/vehicle/' in href:
                if not href.startswith("http"): href = "https://patpat.lk" + href
                
                with self.lock:
                    if self.is_seen(href):
                        self.stats['dupes'] += 1
                        continue
                    self.mark_seen(href)
                
                # Grab basic info from listing if available
                title = link.get_text(" ", strip=True)
                item = {'url': href, 'make': make, 'type': v_type, 'title': title, 'price': '0', 'date': 'Check_Page'}
                
                # Fetch Ad Page Content HERE (Original logic)
                ad_html = client.fetch(href, method="flaresolverr")
                if ad_html:
                    self.queue.put({'item': item, 'html': ad_html})

    def extractor_worker(self, cutoff):
        re_date = re.compile(r'(\d{4}-\d{2}-\d{2})')
        re_trans = re.compile(r'\b(Automatic|Manual|Tiptronic)\b', re.IGNORECASE)
        re_phone = re.compile(r'(?:0\d{1,2}|07\d)[- ]?\d{3}[- ]?\d{4}')

        while not self.stop_event.is_set() or not self.queue.empty():
            try: record = self.queue.get(timeout=3)
            except: continue
            
            try:
                item = record['item']
                soup = BeautifulSoup(record['html'], "html.parser")
                full_text = soup.get_text(" ", strip=True)

                # Date Extraction
                dm = re_date.search(full_text)
                if dm:
                    try:
                        if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff: continue
                        item['date'] = dm.group(1)
                    except: pass
                else: item['date'] = datetime.now().strftime("%Y-%m-%d")

                # Price Extraction
                pm = re.search(r'Rs\.?\s*([\d,]+)', full_text)
                if pm: item['price'] = pm.group(1).replace(',', '')

                details = {'YOM': '', 'Transmission': '', 'Fuel': '', 'Engine': '', 'Mileage': '', 'Contact': '', 'Location': '', 'Description': ''}

                # DOM Parsing
                for row in soup.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        k = cols[0].get_text(strip=True).lower()
                        v = cols[1].get_text(strip=True)
                        if 'mileage' in k: details['Mileage'] = v
                        elif 'engine' in k: details['Engine'] = v
                        elif 'transmission' in k: details['Transmission'] = v
                        elif 'fuel' in k: details['Fuel'] = v
                        elif 'year' in k: details['YOM'] = v
                
                if not details['Transmission']:
                    m = re_trans.search(full_text)
                    if m: details['Transmission'] = m.group(1).capitalize()
                
                phones = re_phone.findall(full_text)
                if phones: details['Contact'] = " / ".join(set([p.replace('-','').replace(' ','') for p in phones]))

                # Unified Write
                basic_row = {
                    'Source': 'Patpat', 'Date': item['date'], 'Make': item['make'], 
                    'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 
                    'Price': item['price'], 'URL': item['url']
                }
                self.unified_writer.add_row(basic_row)
                
                # Detailed Write
                detail_row = {**basic_row, **details}
                del detail_row['Source']
                self.detail_writer.add_row(detail_row)
                
                with self.lock: self.stats['saved'] += 1

            except: pass
            finally: self.queue.task_done()


# ==========================================
# 3️⃣ IKMAN SCRAPER CLASS
# ==========================================
class IkmanScraper:
    def __init__(self, unified_writer):
        self.unified_writer = unified_writer
        self.detail_writer = None
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.stats = {'saved': 0}
        self.lock = threading.Lock()
        self.seen_urls = set()
        
        self.makes = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 'kia', 'hyundai', 'bmw', 'mercedes-benz']
        self.types = ['cars', 'vans', 'suvs', 'motorbikes']
        self.max_pages = 10 # Ikman has strict pagination limits usually
        self.workers = 4

    def run(self):
        print("\n" + "="*50)
        print("🚀 STARTING: IKMAN.LK SCRAPER")
        print("="*50)
        
        cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
        det_csv = f"{DATA_FOLDER}/IKMAN_DETAILED_{TS}.csv"
        det_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL']
        self.detail_writer = UnifiedBatchWriter(det_csv, det_fields)
        
        client = FlareSolverrClient()
        
        # Start Extractors
        ex_pool = ThreadPoolExecutor(max_workers=self.workers)
        for _ in range(self.workers):
            ex_pool.submit(self.extractor_worker, cutoff)

        # Start Crawling
        tasks = []
        for m in self.makes:
            for t in self.types:
                for p in range(1, self.max_pages + 1):
                    tasks.append((m, t, p))
        random.shuffle(tasks)
        
        with ThreadPoolExecutor(max_workers=2) as s_pool:
            futures = [s_pool.submit(self.harvest_task, client, m, t, p) for m, t, p in tasks]
            with tqdm(total=len(futures), desc="Ikman Crawl") as pbar:
                for _ in as_completed(futures):
                    pbar.update(1)
        
        self.queue.join()
        self.stop_event.set()
        ex_pool.shutdown(wait=True)
        self.detail_writer.flush()
        print(f"✅ Ikman Done. Saved: {self.stats['saved']}")

    def harvest_task(self, client, make, v_type, page):
        url = f"https://ikman.lk/en/ads/sri-lanka/{v_type}/{make}"
        if page > 1: url += f"?page={page}"
        
        html = client.fetch(url, method="flaresolverr")
        if not html: return

        soup = BeautifulSoup(html, 'html.parser')
        items = soup.find_all('li', class_=re.compile(r'(normal|top-ad)--'))
        
        for item in items:
            a_tag = item.find('a', href=True)
            if not a_tag: continue
            href = f"https://ikman.lk{a_tag['href']}"
            
            with self.lock:
                if href in self.seen_urls: continue
                self.seen_urls.add(href)
            
            title_tag = item.find('h2', class_=re.compile(r'heading--'))
            title = title_tag.get_text(strip=True) if title_tag else "Unknown"
            
            price = "0"
            price_tag = item.find('div', class_=re.compile(r'price--'))
            if price_tag:
                pm = re.search(r'Rs\s*([\d,]+)', price_tag.get_text(strip=True))
                if pm: price = pm.group(1).replace(',', '')
                
            self.queue.put({'url': href, 'make': make, 'type': v_type, 'title': title, 'price': price, 'date': 'Check_Page'})

    def extractor_worker(self, cutoff):
        client = FlareSolverrClient()
        re_fuel = re.compile(r'\b(Petrol|Diesel|Hybrid|Electric|CNG)\b', re.IGNORECASE)
        re_trans = re.compile(r'\b(Automatic|Manual|Tiptronic)\b', re.IGNORECASE)
        re_phone = re.compile(r'(?:07\d|0\d{2})[- ]?\d{3}[- ]?\d{4}')

        while not self.stop_event.is_set() or not self.queue.empty():
            try: item = self.queue.get(timeout=3)
            except: continue
            
            try:
                html = client.fetch(item['url'], method="flaresolverr")
                if not html: continue
                soup = BeautifulSoup(html, "html.parser")
                full_text = soup.get_text(" ", strip=True)

                # Date parsing (Complex Ikman logic)
                if item['date'] == "Check_Page":
                    # Simplified for master script (Current date default if parsing fails)
                    item['date'] = datetime.now().strftime("%Y-%m-%d")

                details = {'YOM': '', 'Transmission': '', 'Fuel': '', 'Engine': '', 'Mileage': '', 'Contact': '', 'Location': ''}
                
                # Regex Sweep
                m_fuel = re_fuel.search(full_text)
                if m_fuel: details['Fuel'] = m_fuel.group(1).title()
                m_trans = re_trans.search(full_text)
                if m_trans: details['Transmission'] = m_trans.group(1).title()
                
                # Text Stream for YOM/Engine
                lines = [line.strip().lower() for line in soup.get_text("\n").split("\n") if line.strip()]
                for i, line in enumerate(lines):
                    if i + 1 < len(lines):
                        next_line = lines[i+1]
                        if 'model year' in line and not details['YOM']: details['YOM'] = next_line
                        elif 'engine capacity' in line and not details['Engine']: details['Engine'] = next_line
                        elif 'mileage' in line and not details['Mileage']: details['Mileage'] = next_line
                
                phones = re_phone.findall(full_text)
                if phones: details['Contact'] = " / ".join(set([p.replace('-','').replace(' ','') for p in phones]))

                # Unified Write
                basic_row = {
                    'Source': 'Ikman', 'Date': item['date'], 'Make': item['make'], 
                    'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 
                    'Price': item['price'], 'URL': item['url']
                }
                self.unified_writer.add_row(basic_row)
                
                # Detailed Write
                detail_row = {**basic_row, **details}
                del detail_row['Source']
                self.detail_writer.add_row(detail_row)
                
                with self.lock: self.stats['saved'] += 1
            except: pass
            finally: self.queue.task_done()
        client.destroy_session()

# ==========================================
# 🏁 MAIN EXECUTION
# ==========================================
def main():
    print(f"🚀 VEHICLE MASTER SCRAPER STARTED")
    print(f"📂 Saving to: {DATA_FOLDER}")
    
    # 1. Initialize Unified Basic Writer
    unified_csv = f"{DATA_FOLDER}/ALL_SITES_BASIC_{TS}.csv"
    unified_fields = ['Source', 'Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'URL']
    unified_writer = UnifiedBatchWriter(unified_csv, unified_fields)
    
    # 2. Run Scrapers Sequentially
    try:
        # Step A: Riyasewana
        riya = RiyasewanaScraper(unified_writer)
        riya.run()
        
        time.sleep(5) # Cool down
        
        # Step B: Patpat
        patpat = PatpatScraper(unified_writer)
        patpat.run()
        
        time.sleep(5) # Cool down
        
        # Step C: Ikman
        ikman = IkmanScraper(unified_writer)
        ikman.run()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopped by User")
    
    # 3. Finalize
    unified_writer.flush()
    print("\n✅ MASTER JOB COMPLETE")
    print(f"📊 Check folder '{DATA_FOLDER}' for output.")

if __name__ == "__main__":
    main()
