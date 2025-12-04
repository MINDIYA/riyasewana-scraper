#!/usr/bin/env python3
"""
RIYASEWANA SCRAPER - HYBRID V3 (OPTIMIZED)
- OPTIMIZED: Reduced threads + added jitter to prevent IP bans.
- FIX: Global 'SEEN_URLS' memory to stop duplicate promoted ads.
- FIX: Smart Make/Type correction (Toyota on Tata page).
- FIX: Location regex allows hyphens (Ja-Ela).
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
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ==========================================
# ⚙️ CONFIGURATION (OPTIMIZED)
# ==========================================
# NOTICE: High thread counts = Instant Ban = Slow Script.
# These lower numbers will actually result in FASTER total scraping
# because they keep the "Fast Mode" (CloudScraper) active.
SEARCH_WORKERS = 3    
DETAIL_WORKERS = 5   

FLARESOLVERR_URL = "http://localhost:8191/v1"

MAKES = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda', 
         'daihatsu', 'kia', 'hyundai', 'micro', 'audi', 'bmw', 'mercedes-benz', 'land-rover', 'tata', 'mahindra']
TYPES = ['cars', 'vans', 'suvs', 'crew-cabs', 'pickups']

MAX_PAGES_PER_COMBO = 160
DAYS_TO_KEEP = 15
BATCH_SIZE = 50

print("="*60)
print(f"🚀 RIYASEWANA HYBRID SCRAPER V3 (OPTIMIZED)")
print(f"⚡ Search Threads: {SEARCH_WORKERS} | 📥 Extractor Threads: {DETAIL_WORKERS}")
print(f"🔥 Mode: CloudScraper (Primary) -> FlareSolverr (Backup)")
print("="*60)

ad_queue = queue.Queue(maxsize=2000)
stop_event = threading.Event()

# 🛡 GLOBAL MEMORY FOR DUPLICATES
SEEN_URLS = set()
seen_lock = threading.Lock()

stats = {'found': 0, 'saved': 0, 'skipped_date': 0, 'fast_hits': 0, 'slow_hits': 0, 'dupes': 0}
stats_lock = threading.Lock()

class BatchWriter:
    def __init__(self, filepath, fieldnames):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.buffer = []
        self.lock = threading.Lock()
        with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()

    def add_row(self, row):
        with self.lock:
            self.buffer.append(row)
            if len(self.buffer) >= BATCH_SIZE:
                self.flush_unsafe()

    def flush(self):
        with self.lock:
            if self.buffer: self.flush_unsafe()

    def flush_unsafe(self):
        try:
            with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(self.buffer)
            self.buffer = []
        except: pass

class FlareSolverrClient:
    def __init__(self):
        self.url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        # OPTIMIZATION: Mimic Windows Chrome for better trust score
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        self.session_id = None

    def create_session(self):
        try:
            sid = f"riysess_{random.randint(1000,9999)}_{int(time.time())}"
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

    def fetch(self, url):
        # --- METHOD 1: CLOUDSCRAPER (FAST) ---
        try:
            # CRITICAL: Random delay prevents the server from flagging you as a bot
            time.sleep(random.uniform(1.0, 3.0)) 
            
            r = self.scraper.get(url, timeout=20)
            
            if r.status_code == 200 and len(r.text) > 1000:
                # Check if we actually got the content or a Cloudflare landing page
                if "Attention Required!" not in r.text and "Access denied" not in r.text:
                    with stats_lock: stats['fast_hits'] += 1
                    return r.text
        except Exception:
            pass # Silent fail, move to Method 2

        # --- METHOD 2: FLARESOLVERR (SLOW FALLBACK) ---
        with stats_lock: stats['slow_hits'] += 1
        
        if not self.session_id: self.create_session()
        
        payload = {"cmd": "request.get", "url": url, "maxTimeout": 60000}
        if self.session_id: payload["session"] = self.session_id
        
        try:
            # FlareSolverr needs a longer timeout to process the JS challenge
            r = requests.post(self.url, json=payload, headers=self.headers, timeout=65)
            if r.status_code == 200:
                resp = r.json()
                if resp.get("status") == "ok":
                    return resp.get("solution", {}).get("response", "")
        except: pass
        return None

# ---------------------------------------------------------
# WORKER 1: LINK HUNTER (WITH GLOBAL DEDUP)
# ---------------------------------------------------------
def harvest_task(client, make, v_type, page_num, cutoff_date):
    url = f"https://riyasewana.com/search/{v_type}/{make}"
    if page_num > 1: url += f"?page={page_num}"
    
    html = client.fetch(url)
    if not html: return 0

    soup = BeautifulSoup(html, 'html.parser')
    all_links = soup.find_all('a', href=True)
    local_count = 0

    for link in all_links:
        href = link['href']
        if '/buy/' in href and '-sale-' in href:
            
            # 🛡 GLOBAL DUPLICATE CHECK
            with seen_lock:
                if href in SEEN_URLS:
                    with stats_lock: stats['dupes'] += 1
                    continue
                SEEN_URLS.add(href)
            
            title = link.get_text(" ", strip=True)
            if len(title) < 5:
                h2 = link.find_parent('h2')
                if h2: title = h2.get_text(" ", strip=True)

            container = link.find_parent('li')
            if not container: container = link.find_parent('div', class_=re.compile('item'))
            
            final_date = "Check_Page"
            price = "0"

            if container:
                cont_text = container.get_text(" ", strip=True)
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', cont_text)
                if date_match:
                    d_str = date_match.group(1)
                    try:
                        if datetime.strptime(d_str, "%Y-%m-%d") < cutoff_date:
                            with stats_lock: stats['skipped_date'] += 1
                            continue 
                        final_date = d_str
                    except: pass
                
                price_match = re.search(r'Rs\.?\s*([\d,]+)', cont_text)
                if price_match:
                    price = price_match.group(1).replace(',', '')

            item = {'url': href, 'date': final_date, 'make': make, 'type': v_type, 'title': title, 'price': price}
            ad_queue.put(item)
            local_count += 1

    with stats_lock: stats['found'] += local_count
    return local_count

# ---------------------------------------------------------
# WORKER 2: DOM MASTER EXTRACTOR (SMART LOGIC)
# ---------------------------------------------------------
def extractor_worker(basic_writer, detail_writer, cutoff_date):
    client = FlareSolverrClient()
    re_phone = re.compile(r'(?:07\d|0\d{2})[- ]?\d{3}[- ]?\d{4}')
    re_yom = re.compile(r'\b(20\d{2}|19\d{2})\b')
    
    while not stop_event.is_set() or not ad_queue.empty():
        try:
            item = ad_queue.get(timeout=3)
        except queue.Empty: continue

        try:
            html = client.fetch(item['url'])
            if not html: continue

            soup = BeautifulSoup(html, "html.parser")
            full_text = soup.get_text(" ", strip=True)

            final_date = item['date']
            if final_date == "Check_Page":
                dm = re.search(r'(\d{4}-\d{2}-\d{2})', full_text)
                if dm:
                    try:
                        if datetime.strptime(dm.group(1), "%Y-%m-%d") < cutoff_date:
                            with stats_lock: stats['skipped_date'] += 1
                            continue
                        final_date = dm.group(1)
                    except: pass
                else:
                    final_date = datetime.now().strftime("%Y-%m-%d")

            details = {
                'YOM': '', 'Transmission': '', 'Fuel': '', 
                'Engine': '', 'Mileage': '', 'Contact': 'Unknown', 
                'Location': '', 'Description': ''
            }

            # 🔴 SMART AUTO-CORRECTION
            final_make = item['make']
            final_type = item['type']
            title_lower = item['title'].lower()
            
            # 1. FIX MAKE (e.g. Tata -> Mitsubishi)
            if final_make not in title_lower:
                for m in MAKES:
                    if m in title_lower:
                        final_make = m
                        break
            
            # 2. FIX TYPE (Generic Keywords only - Preserves 'Pickup' for Defenders)
            if 'suv' in title_lower or 'jeep' in title_lower: final_type = 'suvs'
            elif 'van' in title_lower: final_type = 'vans'
            elif 'car' in title_lower or 'sedan' in title_lower or 'hatchback' in title_lower: final_type = 'cars'
            elif 'pickup' in title_lower or 'cab' in title_lower: final_type = 'pickups'

            # 🟢 DOM FIX
            labels = soup.find_all('p', class_='moreh')
            for label in labels:
                txt = label.get_text(strip=True).lower()
                parent_td = label.find_parent('td')
                if parent_td:
                    value_td = parent_td.find_next_sibling('td')
                    if value_td:
                        val = value_td.get_text(strip=True)
                        if 'mileage' in txt: details['Mileage'] = val + " km" if "km" not in val.lower() else val
                        elif 'engine' in txt or 'capacity' in txt: details['Engine'] = val + " cc" if "cc" not in val.lower() else val
                        elif 'transmission' in txt: details['Transmission'] = val
                        elif 'fuel' in txt: details['Fuel'] = val
                        elif 'yom' in txt or 'year' in txt: details['YOM'] = val

            if not details['YOM']:
                m_yom = re_yom.search(item['title'])
                if m_yom: details['YOM'] = m_yom.group(1)

            phones = re_phone.findall(full_text)
            if phones:
                clean_phones = list(set([p.replace('-', '').replace(' ', '') for p in phones]))
                details['Contact'] = " / ".join(clean_phones)

            h1 = soup.find('h1')
            if h1 and " in " in h1.get_text():
                details['Location'] = h1.get_text().split(" in ")[-1].strip()
            # 🟢 LOCATION FIX (ALLOWS HYPHENS LIKE JA-ELA)
            elif (loc_m := re.search(r'-sale-([a-zA-Z-]+)-\d', item['url'])):
                details['Location'] = loc_m.group(1).replace('-', ' ').title()

            desc_h = soup.find(string=re.compile(r'Description', re.IGNORECASE))
            if desc_h:
                container = desc_h.find_parent().find_next('div') or desc_h.find_parent().find_next('p')
                if container: details['Description'] = container.get_text(strip=True)[:500]

            row_detailed = {'Date': final_date, 'Make': final_make, 'Type': final_type, 'YOM': details['YOM'], 'Model': item['title'], 'Price': item['price'], 'Transmission': details['Transmission'], 'Fuel': details['Fuel'], 'Engine': details['Engine'], 'Mileage': details['Mileage'], 'Location': details['Location'], 'Contact': details['Contact'], 'URL': item['url']}
            row_basic = {k: v for k, v in row_detailed.items() if k in basic_writer.fieldnames}

            basic_writer.add_row(row_basic)
            detail_writer.add_row(row_detailed)
            with stats_lock: stats['saved'] += 1

        except: pass
        finally: ad_queue.task_done()
    
    client.destroy_session()

def main():
    cutoff = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    folder = "riyasewana_dom_data"
    if not os.path.exists(folder): os.makedirs(folder)
    
    ts = time.strftime('%Y-%m-%d_%H-%M')
    basic_csv = f"{folder}/RIYA_BASIC_{ts}.csv"
    detail_csv = f"{folder}/RIYA_DETAILED_{ts}.csv"
    
    basic_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price']
    detail_fields = ['Date', 'Make', 'Type', 'YOM', 'Model', 'Price', 'Transmission', 'Fuel', 'Engine', 'Mileage', 'Location', 'Contact', 'URL']

    bw = BatchWriter(basic_csv, basic_fields)
    dw = BatchWriter(detail_csv, detail_fields)
    client_test = FlareSolverrClient()
    
    print("🚀 Starting Hybrid Extraction...")
    
    ex_pool = ThreadPoolExecutor(max_workers=DETAIL_WORKERS)
    for _ in range(DETAIL_WORKERS):
        ex_pool.submit(extractor_worker, bw, dw, cutoff)
        time.sleep(0.1)

    tasks = []
    for make in MAKES:
        for v_type in TYPES:
            for page in range(1, MAX_PAGES_PER_COMBO + 1):
                tasks.append((make, v_type, page))
    random.shuffle(tasks)
    
    with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as s_pool:
        futures = []
        for (m, t, p) in tasks:
            futures.append(s_pool.submit(harvest_task, client_test, m, t, p, cutoff))
        
        with tqdm(total=len(futures), desc="Crawling", unit="pg") as pbar:
            for _ in as_completed(futures):
                pbar.update(1)
                pbar.set_postfix({
                    "Sv": stats['saved'], 
                    "Dup": stats['dupes'], 
                    "Fast": stats['fast_hits'],
                    "Slow": stats['slow_hits']
                })

    ad_queue.join()
    stop_event.set()
    ex_pool.shutdown(wait=True)
    bw.flush()
    dw.flush()
    print("\n✅ DONE")

if __name__ == "__main__":
    main()
