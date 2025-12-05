#!/usr/bin/env python3
"""
SRI LANKA VEHICLE MASTER SCRAPER (AUTO-RETRY EDITION)
- LOGIC: If CloudScraper returns 0 ads, immediately retry with Docker.
- DIAGNOSTIC: Checks Docker health at startup.
- OUTPUT: Saves to CSV instantly.
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
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
FLARESOLVERR_URL = "http://localhost:8191/v1"
DAYS_TO_KEEP = 15
BATCH_SIZE = 1  # Instant saving
DATA_FOLDER = "vehicle_data_master"

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

TS = time.strftime('%Y-%m-%d_%H-%M')

# ==========================================
# 🛠️ SHARED TOOLS
# ==========================================

class UnifiedBatchWriter:
    def __init__(self, filename, fieldnames):
        self.filepath = filename
        self.fieldnames = fieldnames
        self.lock = threading.Lock()
        
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def add_row(self, row):
        with self.lock:
            try:
                clean_row = {k: row.get(k, '') for k in self.fieldnames}
                with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writerow(clean_row)
                    f.flush()
            except Exception as e:
                print(f"❌ Write Error: {e}")

class FlareSolverrClient:
    """Smart Hybrid Client with Explicit Methods"""
    def __init__(self):
        self.url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        self.session_id = None

    def check_health(self):
        """Diagnostics: Check if Docker is alive"""
        try:
            r = requests.get(self.url, timeout=5)
            # FlareSolverr root usually returns generic JSON or 404, but connection means it's up
            return True
        except:
            return False

    def create_session(self):
        try:
            sid = f"sess_{random.randint(1000,9999)}_{int(time.time())}"
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
        # METHOD 1: CLOUDSCRAPER
        if method == "hybrid" or method == "cloudscraper":
            try:
                time.sleep(random.uniform(1.5, 3.0))
                r = self.scraper.get(url, timeout=15)
                if r.status_code == 200 and len(r.text) > 1000:
                    if "Just a moment" not in r.text and "Attention Required" not in r.text:
                        return r.text
            except: pass
            
            # If explicitly requested cloudscraper and it failed, return None (don't auto fallback here)
            if method == "cloudscraper": return None

        # METHOD 2: FLARESOLVERR (Docker)
        if not self.session_id: self.create_session()
        
        payload = {
            "cmd": "request.get", 
            "url": url, 
            "maxTimeout": 60000, 
            "session": self.session_id
        }
        
        try:
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=65)
            if r.status_code == 200:
                resp = r.json()
                if resp.get("status") == "ok":
                    return resp.get("solution", {}).get("response", "")
        except: pass
        return None




# ==========================================
# 2️⃣ PATPAT SCRAPER
# ==========================================
class PatpatScraper:
    def __init__(self, unified_writer):
        self.unified_writer = unified_writer
        self.detail_writer = None
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.stats = {'saved': 0, 'dupes': 0, 'errors': 0}
        self.lock = threading.Lock()
        
        self.db_path = f"{DATA_FOLDER}/patpat_seen.sqlite"
        self._init_db()
        
        self.makes = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 'kia', 'hyundai', 'micro', 'audi', 'bmw', 'tata']
        self.types = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']
        self.max_pages = 160
        self.workers = 5

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
        
        ex_pool = ThreadPoolExecutor(max_workers=self.workers)
        for _ in range(self.workers):
            ex_pool.submit(self.extractor_worker, cutoff)
            
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
                    pbar.set_postfix({"Saved": self.stats['saved']})
        
        self.queue.join()
        self.stop_event.set()
        ex_pool.shutdown(wait=True)
        self.conn.close()
        print(f"✅ Patpat Done. Saved: {self.stats['saved']}")

    def harvest_task(self, client, make, v_type, page, cutoff):
        url = f"https://patpat.lk/search/{v_type}/{make}?page={page}"
        html = client.fetch(url, method="flaresolverr") # Strict docker for Patpat
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
                
                title = link.get_text(" ", strip=True)
                item = {'url': href, 'make': make, 'type': v_type, 'title': title, 'price': '0', 'date': 'Check_Page'}
                
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

                dm = re_date.search(full_text)
                if dm:
                    try:
                        if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff: continue
                        item['date'] = dm.group(1)
                    except: pass
                else: item['date'] = datetime.now().strftime("%Y-%m-%d")

                pm = re.search(r'Rs\.?\s*([\d,]+)', full_text)
                if pm: item['price'] = pm.group(1).replace(',', '')

                details = {'YOM': '', 'Transmission': '', 'Fuel': '', 'Engine': '', 'Mileage': '', 'Contact': '', 'Location': '', 'Description': ''}

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

                basic_row = {
                    'Source': 'Patpat', 'Date': item['date'], 'Make': item['make'], 
                    'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 
                    'Price': item['price'], 'URL': item['url']
                }
                
                print(f"✅ Saved: {item['title']}")
                self.unified_writer.add_row(basic_row)
                
                detail_row = {**basic_row, **details}
                if 'Source' in detail_row: del detail_row['Source']
                self.detail_writer.add_row(detail_row)
                
                with self.lock: self.stats['saved'] += 1

            except: 
                with self.lock: self.stats['errors'] += 1
            finally: self.queue.task_done()


# ==========================================
# 3️⃣ IKMAN SCRAPER
# ==========================================
class IkmanScraper:
    def __init__(self, unified_writer):
        self.unified_writer = unified_writer
        self.detail_writer = None
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.stats = {'saved': 0, 'errors': 0}
        self.lock = threading.Lock()
        self.seen_urls = set()
        
        self.makes = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 'kia', 'hyundai', 'bmw', 'mercedes-benz']
        self.types = ['cars', 'vans', 'suvs', 'motorbikes']
        self.max_pages = 10 
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
        
        ex_pool = ThreadPoolExecutor(max_workers=self.workers)
        for _ in range(self.workers):
            ex_pool.submit(self.extractor_worker, cutoff)

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
                    pbar.set_postfix({"Saved": self.stats['saved']})
        
        self.queue.join()
        self.stop_event.set()
        ex_pool.shutdown(wait=True)
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

                if item['date'] == "Check_Page":
                    item['date'] = datetime.now().strftime("%Y-%m-%d")

                details = {'YOM': '', 'Transmission': '', 'Fuel': '', 'Engine': '', 'Mileage': '', 'Contact': '', 'Location': ''}
                
                m_fuel = re_fuel.search(full_text)
                if m_fuel: details['Fuel'] = m_fuel.group(1).title()
                m_trans = re_trans.search(full_text)
                if m_trans: details['Transmission'] = m_trans.group(1).title()
                
                lines = [line.strip().lower() for line in soup.get_text("\n").split("\n") if line.strip()]
                for i, line in enumerate(lines):
                    if i + 1 < len(lines):
                        next_line = lines[i+1]
                        if 'model year' in line and not details['YOM']: details['YOM'] = next_line
                        elif 'engine capacity' in line and not details['Engine']: details['Engine'] = next_line
                        elif 'mileage' in line and not details['Mileage']: details['Mileage'] = next_line
                
                phones = re_phone.findall(full_text)
                if phones: details['Contact'] = " / ".join(set([p.replace('-','').replace(' ','') for p in phones]))

                basic_row = {
                    'Source': 'Ikman', 'Date': item['date'], 'Make': item['make'], 
                    'Type': item['type'], 'YOM': details['YOM'], 'Model': item['title'], 
                    'Price': item['price'], 'URL': item['url']
                }
                
                print(f"✅ Saved: {item['title']}")
                self.unified_writer.add_row(basic_row)
                
                detail_row = {**basic_row, **details}
                if 'Source' in detail_row: del detail_row['Source']
                self.detail_writer.add_row(detail_row)
                
                with self.lock: self.stats['saved'] += 1
            except: 
                with self.lock: self.stats['errors'] += 1
            finally: self.queue.task_done()
        client.destroy_session()

# ==========================================
# 🏁 MAIN EXECUTION
# ==========================================
def main():
    print(f"🚀 VEHICLE MASTER SCRAPER (AUTO-RETRY MODE) STARTED")
    print(f"📂 Saving to: {DATA_FOLDER}")
    
    unified_csv = f"{DATA_FOLDER}/ALL_SITES_BASIC_{TS}.csv"
    unified_fields = ['Source', 'Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'URL']
    unified_writer = UnifiedBatchWriter(unified_csv, unified_fields)
    
    try:
        
        patpat = PatpatScraper(unified_writer)
        patpat.run()
        
        print("⏳ Cooling down 10s...")
        time.sleep(10)
        
        ikman = IkmanScraper(unified_writer)
        ikman.run()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopped by User")
    
    print("\n✅ MASTER JOB COMPLETE")

if __name__ == "__main__":
    main()
