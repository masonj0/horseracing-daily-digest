#!/usr/bin/env python3
"""
AUS/NZ Racing Report Generator (v1.0 - Final)

This script generates a specialized report of upcoming small-field races in
Australia and New Zealand. It uses Sporting Life for a master race list and
enriches Australian races with live odds from the AtTheRaces API.
"""

# --- Core Python Libraries ---
import os
import re
from datetime import datetime, date
from urllib.parse import urljoin, urlparse

# --- Third-Party Libraries ---
import pytz
import requests
from bs4 import BeautifulSoup

# --- Suppress SSL Warnings ---
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

TIMEZONE_MAP = {
    'AUS': 'Australia/Sydney',
    'NZ': 'Pacific/Auckland',
    # Keep others for potential future expansion
    'UK': 'Europe/London', 'IRE': 'Europe/Dublin', 'FR': 'Europe/Paris',
    'SAF': 'Africa/Johannesburg', 'USA': 'America/New_York', 'CAN': 'America/Toronto',
}

COURSE_TO_COUNTRY_MAP = {
    # Australia
    'eagle farm': 'AUS', 'moonee valley': 'AUS', 'morphettville': 'AUS', 'newcastle': 'AUS',
    'gold coast': 'AUS', 'gold coast poly': 'AUS', 'pinjarra': 'AUS', 'randwick': 'AUS',
    'flemington': 'AUS', 'caulfield': 'AUS',
    # New Zealand
    'riccarton': 'NZ', 'te rapa': 'NZ',
    # Other examples for robustness
    'haydock': 'UK', 'newmarket': 'UK', 'ascot': 'UK', 'curragh': 'IRE', 'deauville': 'FR',
    'turffontein': 'SAF', 'saratoga': 'USA', 'woodbine': 'CAN',
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def normalize_track_name(name: str) -> str:
    if not isinstance(name, str): return ""
    return name.lower().strip().replace('(july)', '').replace('(aw)', '').replace('acton', '').replace('park', '').strip()

def fetch_page(url: str):
    print(f"-> Fetching: {url}")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=20)
        response.raise_for_status()
        print("   ✅ Success.")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Failed to fetch page: {e}")
        return None

def sort_and_limit_races(races: list[dict], limit: int = 25) -> list[dict]:
    print(f"\n⏳ Filtering for upcoming races, sorting, and limiting...")
    now_utc = datetime.now(pytz.utc)
    future_races = [race for race in races if race.get('datetime_utc') and race['datetime_utc'] > now_utc]
    future_races.sort(key=lambda r: r['datetime_utc'])
    limited_races = future_races[:limit]
    print(f"   -> Found {len(future_races)} upcoming races. Limiting to a maximum of {len(limited_races)}.")
    return limited_races

# ==============================================================================
# STEP 1: UNIVERSAL SCAN
# ==============================================================================

def universal_sporting_life_scan(html_content: str, base_url: str, today_date: date):
    if not html_content: return []
    print("\n🔍 Starting Universal Scan of Sporting Life...")
    soup = BeautifulSoup(html_content, 'html.parser')
    all_races, processed = [], set()
    for link in soup.find_all('a', href=re.compile(r'/racing/racecards/....-..-../.*/racecard/')):
        try:
            parts = urlparse(link.get('href')).path.strip('/').split('/'); idx = parts.index('racecards')
            date_url, course = parts[idx+1], parts[idx+2].replace('-',' ').title()
        except (ValueError, IndexError): continue
        parent = link.parent
        if not parent: continue
        race_time = None
        time_tag = parent.find_previous_sibling('span')
        if time_tag and (match := re.search(r'(\d{2}:\d{2})', time_tag.text)): race_time = match.group(1)
        if not race_time and (gparent := parent.parent) and (match := re.search(r'(\d{2}:\d{2})', gparent.text)): race_time = match.group(1)
        if not race_time: continue
        key = (normalize_track_name(course), race_time)
        if key in processed: continue
        processed.add(key)
        runners = re.search(r'(\d+)\s+Runners', link.get_text(strip=True), re.IGNORECASE)
        field = int(runners.group(1)) if runners else 0
        country = COURSE_TO_COUNTRY_MAP.get(normalize_track_name(course), 'UK')
        utc_time, date_iso = None, None
        try:
            tz = pytz.timezone(TIMEZONE_MAP.get(country, 'Europe/London'))
            naive = datetime.strptime(f"{date_url} {race_time}", '%Y-%m-%d %H:%M')
            utc_time = tz.localize(naive).astimezone(pytz.utc)
            date_iso = naive.strftime('%d-%m-%Y')
        except (ValueError, KeyError): continue
        all_races.append({'course': course, 'time': race_time, 'field_size': field, 'race_url': urljoin(base_url, link.get('href')),
                          'country': country, 'date_iso': date_iso, 'datetime_utc': utc_time})
        print(f"   -> Found Today's Race: {course} ({country}) at {race_time} [{tz.zone}]")
    print(f"✅ Sporting Life Scan complete. Found {len(all_races)} races for today.")
    return all_races

# ==============================================================================
# STEP 2: ODDS ENRICHMENT & REPORTING
# ==============================================================================

def convert_odds_to_float(odds_str: str) -> float:
    if not isinstance(odds_str, str): return 9999.0
    s = odds_str.strip().upper()
    if 'SP' in s: return 9999.0
    if s == 'EVS': return 1.0
    if '/' in s:
        try: n, d = map(float, s.split('/')); return n/d if d != 0 else 9999.0
        except (ValueError, IndexError): return 9999.0
    try: return float(s)
    except ValueError: return 9999.0

def fetch_atr_odds_data(regions: list[str]) -> dict:
    print("\n📡 Fetching Live Odds from AtTheRaces...")
    today_str = datetime.now().strftime('%Y%m%d'); lookup = {}
    for region in regions:
        url = f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{today_str}"; print(f"-> Querying {region.upper()} from: {url}")
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=15); r.raise_for_status()
            if not r.text: continue
        except requests.exceptions.RequestException as e: print(f"   ❌ ERROR: {e}"); continue
        soup = BeautifulSoup(r.text, 'html.parser')
        for caption in soup.find_all('caption', string=re.compile(r"^\d{2}:\d{2}")):
            time_match = re.match(r"(\d{2}:\d{2})", caption.get_text(strip=True))
            if not time_match: continue
            race_time = time_match.group(1)
            course_header = caption.find_parent('div', class_='panel').find('h2')
            if not course_header: continue
            course_name = course_header.get_text(strip=True)
            table = caption.find_next_sibling('table')
            if not table: continue
            horses = [{'name': c[0].get_text(strip=True), 'odds_str': c[1].get_text(strip=True)} for row in table.find('tbody').find_all('tr') if (c := row.find_all('td'))]
            if not horses: continue
            for h in horses: h['odds_float'] = convert_odds_to_float(h['odds_str'])
            horses.sort(key=lambda x: x['odds_float'])
            lookup[(normalize_track_name(course_name), race_time)] = {'course': course_name, 'time': race_time, 'field_size': len(horses),
                'favorite': horses[0] if horses else None, 'second_favorite': horses[1] if len(horses) > 1 else None}
    print(f"✅ AtTheRaces scan complete. Found data for {len(lookup)} races.")
    return lookup

def generate_aus_nz_report(races: list[dict]):
    title = "AUS/NZ Small-Field Races"
    filename = f"AUS_NZ_Small_Field_Report_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.html"
    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f4f4f9; color: #333; margin: 20px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        h1 { color: #5a2d82; text-align: center; border-bottom: 3px solid #5a2d82; padding-bottom: 10px; }
        .race-card { border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-left: 5px solid #5a2d82; background-color: #fff; border-radius: 8px; }
        .race-header { font-size: 1.5em; font-weight: bold; color: #333; margin-bottom: 15px; } .race-meta { font-size: 1.1em; color: #5f6368; margin-bottom: 15px; }
        .horse-details { margin-top: 10px; padding: 10px; border-radius: 5px; background-color: #f8f9fa; } .horse-details b { color: #5a2d82; }
        .no-odds { color: #999; } .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""
    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""
    if not races:
        html_body += "<p>No upcoming AUS/NZ races with 3 to 6 runners were found.</p>"
    else:
        html_body += f"<p>Found {len(races)} upcoming AUS/NZ races with 3 to 6 runners.</p>"
        for race in races:
            html_body += f"""<div class="race-card">
                <div class="race-header">{race['course']} ({race['country']}) - {race['time']}</div>
                <div class="race-meta">Field Size: {race['field_size']} Runners</div>"""
            if race.get('favorite'):
                fav, sec_fav = race['favorite'], race.get('second_favorite')
                html_body += f"""<div class="horse-details"><b>Favorite:</b> {fav['name']} (<b>{fav['odds_str']}</b>)</div>"""
                if sec_fav: html_body += f"""<div class="horse-details"><b>2nd Favorite:</b> {sec_fav['name']} (<b>{sec_fav['odds_str']}</b>)</div>"""
            else:
                html_body += '<div class="horse-details no-odds">Live odds not available for this race.</div>'
            html_body += '</div>'
    html_end = f'<div class="footer"><p>Report generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p></div></div></body></html>'
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(html_start + html_body + html_end)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e: print(f"\n❌ Error saving the report: {e}")

# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================

DATA_SOURCES = [
    {"name": "Sporting Life", "url": "https://www.sportinglife.com/racing/racecards", "scraper": universal_sporting_life_scan},
]

def main():
    print("=" * 80); print("🚀 AUS/NZ Racing Report"); print("=" * 80)
    user_tz = pytz.timezone("Australia/Sydney")
    today_date = datetime.now(user_tz).date()
    print(f"📅 Operating on Date: {today_date.strftime('%Y-%m-%d')}")
    master_race_list = []
    for source in DATA_SOURCES:
        print(f"\n--- Processing source: {source['name']} ---")
        html_content = fetch_page(source['url'])
        if html_content: master_race_list.extend(source['scraper'](html_content, source['url'], today_date))
    print(f"\nTotal unique races found from all sources: {len(master_race_list)}")
    print("\nFiltering for AUS/NZ races...")
    aus_nz_races = [r for r in master_race_list if r.get('country') in ['AUS', 'NZ']]
    print(f"   -> Found {len(aus_nz_races)} unique races from Australia and New Zealand.")
    if not aus_nz_races: print("\nCould not retrieve any AUS/NZ races for today. Exiting."); return
    print("\n-- Processing AUS/NZ Races --")
    small_field_races = [r for r in aus_nz_races if 3 <= r.get('field_size', 0) <= 6]
    print(f"Found {len(small_field_races)} AUS/NZ races with 3 to 6 runners.")
    if not small_field_races: generate_aus_nz_report([]); return
    atr_odds_data = fetch_atr_odds_data(['aus'])
    enriched_races = []
    for race in small_field_races:
        key = (normalize_track_name(race['course']), race['time'])
        if race['country'] == 'AUS' and key in atr_odds_data:
            atr_data = atr_odds_data[key]
            race['field_size'] = atr_data.get('field_size', race['field_size'])
            race['favorite'] = atr_data.get('favorite')
            race['second_favorite'] = atr_data.get('second_favorite')
        enriched_races.append(race)
    final_races = sort_and_limit_races(enriched_races)
    generate_aus_nz_report(final_races)

if __name__ == "__main__":
    main()
