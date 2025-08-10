#!/usr/bin/env python3
"""
Unified Racing Report Generator (v14.0 - Final)

This script generates one of two valuable horse racing reports based on the
network environment. It scrapes race data from multiple sources, merges the
results, and performs a time-zone and date-aware analysis.

- Unrestricted Mode: Finds high-value bets by analyzing live odds.
- Restricted Mode: Provides a rich, actionable list of upcoming small-field
  races with deep links to Sky Sports, R&S, Brisnet, and AtTheRaces.
"""

# --- Core Python Libraries ---
import os
import re
import time
import json
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, urljoin

# --- Third-Party Libraries ---
import requests
import pytz
from bs4 import BeautifulSoup

# --- Suppress SSL Warnings ---
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

TIMEZONE_MAP = {
    'UK': 'Europe/London', 'IRE': 'Europe/Dublin', 'FR': 'Europe/Paris',
    'SAF': 'Africa/Johannesburg', 'USA': 'America/New_York', 'CAN': 'America/Toronto',
    'ARG': 'America/Argentina/Buenos_Aires', 'URU': 'America/Montevideo', 'AUS': 'Australia/Sydney',
}

COURSE_TO_COUNTRY_MAP = {
    'haydock': 'UK', 'newmarket': 'UK', 'ascot': 'UK', 'redcar': 'UK', 'ayr': 'UK',
    'lingfield': 'UK', 'lingfield park': 'UK', 'haydock park': 'UK', 'wolverhampton': 'UK',
    'curragh': 'IRE', 'kilbeggan': 'IRE', 'tipperary': 'IRE',
    'argentan': 'FR', 'deauville': 'FR', 'enghien': 'FR', 'pau': 'FR',
    'turffontein': 'SAF', 'saratoga': 'USA', 'canterbury park': 'USA', 'charles town': 'USA',
    'colonial downs': 'USA', 'del mar': 'USA', 'delaware park': 'USA',
    'ellis park': 'USA', 'fairmount park': 'USA', 'gulfstream': 'USA', 'gulfstream park': 'USA',
    'monmouth park': 'USA', 'remington park': 'USA', 'finger lakes': 'USA', 'woodbine': 'CAN',
    'san isidro': 'ARG', 'maronas': 'URU',
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

def sort_and_limit_races(races: list[dict], limit: int = 20) -> list[dict]:
    print(f"\n⏳ Filtering for upcoming races, sorting, and limiting...")
    now_utc = datetime.now(pytz.utc)
    future_races = [race for race in races if race.get('datetime_utc') and race['datetime_utc'] > now_utc]
    future_races.sort(key=lambda r: r['datetime_utc'])
    limited_races = future_races[:limit]
    print(f"   -> Found {len(future_races)} upcoming races. Limiting to a maximum of {len(limited_races)}.")
    return limited_races

# ==============================================================================
# STEP 1: UNIVERSAL SCANS
# ==============================================================================

def universal_sky_sports_scan(html_content: str, base_url: str, today_date: date):
    if not html_content: return []
    print("\n🔍 Starting Universal Scan of Sky Sports...")
    soup = BeautifulSoup(html_content, 'html.parser')
    all_races, source_tz = [], pytz.timezone('Europe/London')
    def get_country_code(title): return re.search(r'\((\w+)\)$', title).group(1).upper() if re.search(r'\((\w+)\)$', title) else "UK"
    def parse_url(url):
        try:
            parts = urlparse(url).path.strip('/').split('/'); idx = parts.index('racecards')
            return parts[idx+1].replace('-',' ').title(), parts[idx+2]
        except (ValueError, IndexError): return None, None
    for block in soup.find_all('div', class_='sdc-site-concertina-block'):
        title_tag = block.find('h3', class_='sdc-site-concertina-block__title')
        if not title_tag: continue
        country = get_country_code(title_tag.get_text(strip=True))
        events = block.find('div', class_='sdc-site-racing-meetings__events')
        if not events: continue
        for container in events.find_all('div', class_='sdc-site-racing-meetings__event'):
            link = container.find('a', class_='sdc-site-racing-meetings__event-link')
            if not link: continue
            url, (course, date_str) = urljoin(base_url, link.get('href')), parse_url(urljoin(base_url, link.get('href')))
            if not course or not date_str: continue
            try:
                if datetime.strptime(date_str, '%d-%m-%Y').date() != today_date: continue
            except ValueError: continue
            details = container.find('span', class_='sdc-site-racing-meetings__event-details')
            runners = re.search(r'(\d+)\s+runners?', details.get_text(strip=True), re.IGNORECASE) if details else None
            field = int(runners.group(1)) if runners else 0
            time_tag = container.find('span', class_='sdc-site-racing-meetings__event-time')
            race_time = time_tag.get_text(strip=True) if time_tag else "N/A"
            utc_time = None
            if race_time != "N/A":
                try: utc_time = source_tz.localize(datetime.strptime(f"{date_str} {race_time}", '%d-%m-%Y %H:%M')).astimezone(pytz.utc)
                except (ValueError, KeyError): pass
            all_races.append({'course': course, 'time': race_time, 'field_size': field, 'race_url': url,
                              'country': country, 'date_iso': date_str, 'datetime_utc': utc_time})
            print(f"   -> Found Today's Race: {course} ({country}) at {race_time} [Europe/London]")
    print(f"✅ Sky Sports Scan complete. Found {len(all_races)} races for today.")
    return all_races

def universal_sporting_life_scan(html_content: str, base_url: str, today_date: date):
    if not html_content: return []
    print("\n🔍 Starting Universal Scan of Sporting Life...")
    soup = BeautifulSoup(html_content, 'html.parser')
    all_races, processed = [], set()
    for link in soup.find_all('a', href=re.compile(r'/racing/racecards/....-..-../.*/racecard/')):
        try:
            parts = urlparse(link.get('href')).path.strip('/').split('/'); idx = parts.index('racecards')
            date_url, course = parts[idx+1], parts[idx+2].replace('-',' ').title()
            if datetime.strptime(date_url, '%Y-%m-%d').date() != today_date: continue
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
# STEP 2 & MODE A (UNCHANGED)
# ==============================================================================

def check_attheraces_connectivity(url="https://www.attheraces.com/"):
    print("\n🌐 Performing Environmental Check..."); print(f"-> Pinging: {url}")
    try:
        r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, verify=False, timeout=10, stream=True); r.raise_for_status()
        print(f"   ✅ Success! Network is UNRESTRICTED (Status: {r.status_code})."); return True
    except requests.exceptions.RequestException as e:
        print(f"   ❌ AtTheRaces is unreachable. Network is RESTRICTED. Reason: {e}"); return False

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

def generate_mode_A_report(races: list[dict]):
    title = "Perfect Tipsheet"; filename = f"Perfect_Tipsheet_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.html"
    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f4f8; color: #333; margin: 20px; }
        .container { max-width: 800px; margin: auto; background: #fff; padding: 25px; border-radius: 10px; box-shadow: 0 5px 15px rgba(0,0,0,0.1); }
        h1 { color: #1a73e8; text-align: center; border-bottom: 3px solid #1a73e8; padding-bottom: 10px; }
        .race-card { border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-left: 5px solid #1a73e8; background-color: #fff; border-radius: 8px; }
        .race-header { font-size: 1.5em; font-weight: bold; color: #333; margin-bottom: 15px; } .race-meta { font-size: 1.1em; color: #5f6368; margin-bottom: 15px; }
        .horse-details { margin-top: 10px; padding: 10px; border-radius: 5px; background-color: #f8f9fa; } .horse-details b { color: #1a73e8; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""
    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""
    if not races: html_body += "<p>No upcoming races in the next 30 minutes met the specified criteria of Field Size between 3 and 6, Favorite >= 1/1, and 2nd Favorite >= 3/1.</p>"
    else:
        html_body += f"<p>Found {len(races)} races that meet all analytical criteria.</p>"
        for race in races:
            fav, sec_fav = race['favorite'], race['second_favorite']
            html_body += f"""<div class="race-card">
                <div class="race-header">{race['course']} - Race Time: {race['time']}</div>
                <div class="race-meta">Field Size: {race['field_size']} Runners</div>
                <div class="horse-details"><b>Favorite:</b> {fav['name']} (<b>{fav['odds_str']}</b>)</div>
                <div class="horse-details"><b>2nd Favorite:</b> {sec_fav['name']} (<b>{sec_fav['odds_str']}</b>)</div></div>"""
    html_end = f'<div class="footer"><p>Report generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p></div></div></body></html>'
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(html_start + html_body + html_end)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e: print(f"\n❌ Error saving the report: {e}")

def run_mode_A(master_race_list: list[dict]):
    print("\n-- Running Mode A: Unrestricted Workflow --")
    small_field_races = [r for r in master_race_list if 3 <= r.get('field_size', 0) <= 6]
    print(f"Found {len(small_field_races)} races with 3 to 6 runners.")
    if not small_field_races: generate_mode_A_report([]); return
    atr_regions = ['uk', 'ireland', 'usa', 'france', 'saf', 'aus']
    atr_odds_data = fetch_atr_odds_data(atr_regions)
    if not atr_odds_data: print("Could not fetch any live odds from AtTheRaces."); return
    print("\n🔍 Analyzing races against final criteria (next 30 mins, Fav >= 1/1, 2nd Fav >= 3/1)...")
    now_utc, end_time = datetime.now(pytz.utc), datetime.now(pytz.utc) + timedelta(minutes=30)
    perfect_tips = []
    for race in small_field_races:
        if not (race['datetime_utc'] and now_utc < race['datetime_utc'] < end_time): continue
        key = (normalize_track_name(race['course']), race['time'])
        if key in atr_odds_data:
            atr_data = atr_odds_data[key]
            fav, sec_fav = atr_data.get('favorite'), atr_data.get('second_favorite')
            if not (fav and sec_fav): continue
            if fav['odds_float'] >= 1.0 and sec_fav['odds_float'] >= 3.0:
                race['favorite'], race['second_favorite'] = fav, sec_fav
                perfect_tips.append(race)
                print(f"   ✅ MATCH: {race['course']} {race['time']}")
    perfect_tips.sort(key=lambda r: r['datetime_utc'])
    generate_mode_A_report(perfect_tips)

# ==============================================================================
# MODE B: RESTRICTED WORKFLOW (WITH NEW LINK GENERATION)
# ==============================================================================

class RacingAndSportsFetcher:
    def __init__(self, api_url):
        self.api_url = api_url; self.session = requests.Session()
        headers = {'User-Agent':'Mozilla/5.0', 'Accept':'application/json, text/plain, */*', 'Referer':'https://www.racingandsports.com.au/todays-racing'}
        self.session.headers.update(headers)
    def fetch_data(self):
        print("-> Fetching R&S main meeting directory...")
        try: r = self.session.get(self.api_url, timeout=30, verify=False); r.raise_for_status(); return r.json()
        except requests.exceptions.RequestException as e: print(f"   ❌ ERROR: Could not fetch R&S JSON: {e}")
        except json.JSONDecodeError: print("   ❌ ERROR: Failed to decode R&S JSON.")
        return None
    def process_meetings_data(self, json_data):
        if not isinstance(json_data, list): return None
        meetings = []
        for discipline in json_data:
            for country in discipline.get("Countries", []):
                for meeting in country.get("Meetings", []):
                    if (course := meeting.get("Course")) and (link := meeting.get("PDFUrl") or meeting.get("PreMeetingUrl")):
                        meetings.append({'course': course, 'link': link})
        return meetings

def build_rs_lookup_table(rs_meetings):
    lookup = {}
    if not rs_meetings: return lookup
    print("...Building Racing & Sports lookup table for matching...")
    for meeting in rs_meetings:
        link = meeting.get('link')
        match = re.search(r'/(\d{4}-\d{2}-\d{2})', link)
        if not match: continue
        lookup[(normalize_track_name(meeting['course']), match.group(1))] = link
    print(f"   ✅ Lookup table created with {len(lookup)} R&S entries.")
    return lookup

def find_rs_link(track: str, date_iso: str, lookup: dict):
    try: date_yyyymmdd = datetime.strptime(date_iso, '%d-%m-%Y').strftime('%Y-%m-%d')
    except (ValueError, TypeError): return None
    norm_track = normalize_track_name(track)
    if (direct_key := (norm_track, date_yyyymmdd)) in lookup: return lookup[direct_key]
    for (rs_track, rs_date), rs_link in lookup.items():
        if rs_date == date_yyyymmdd and (norm_track in rs_track or rs_track in norm_track): return rs_link
    return None

def generate_external_links(race: dict) -> dict:
    """Generates the Brisnet and AtTheRaces links for a given race."""
    course, date_iso = race.get('course'), race.get('date_iso')
    if not course or not date_iso: return race
    try:
        date_obj = datetime.strptime(date_iso, '%d-%m-%Y')
        # Brisnet: Churchill-Downs / 2023-11-22
        brisnet_course = course.replace(' ', '-')
        brisnet_date = date_obj.strftime('%Y-%m-%d')
        race['brisnet_url'] = f"https://www.brisnet.com/racings-entries-results/USA/{brisnet_course}/{brisnet_date}"
        # AtTheRaces: saratoga / 2023-07-20
        atr_course = course.replace(' ', '-').lower()
        atr_date = date_obj.strftime('%Y-%m-%d')
        race['atr_url'] = f"https://www.attheraces.com/racecards/{atr_course}/{atr_date}"
    except (ValueError, TypeError): pass
    return race

def generate_mode_B_report(races: list[dict]):
    title = "Upcoming Small-Field Races"; filename = f"Actionable_Link_List_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.html"
    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f4f4f9; color: #333; margin: 20px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        h1 { color: #5a2d82; text-align: center; border-bottom: 3px solid #5a2d82; padding-bottom: 10px; }
        .course-group { margin-bottom: 30px; } .course-header { font-size: 1.6em; font-weight: bold; color: #333; padding-bottom: 10px; border-bottom: 2px solid #eee; margin-bottom: 15px; }
        .race-entry { border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fafafa; }
        .race-details { font-weight: bold; font-size: 1.1em; color: #333; margin-bottom: 10px; }
        .race-links a, .race-links span { display: inline-block; text-decoration: none; padding: 8px 15px; border-radius: 4px; margin: 5px 5px 5px 0; font-weight: bold; min-width: 130px; text-align: center; }
        a.sky-link { background-color: #007bff; color: white; } a.sky-link:hover { background-color: #0056b3; }
        a.rs-link { background-color: #dc3545; color: white; } a.rs-link:hover { background-color: #c82333; }
        a.atr-link { background-color: #ffc107; color: black; } a.atr-link:hover { background-color: #e0a800; }
        a.brisnet-link { background-color: #28a745; color: white; } a.brisnet-link:hover { background-color: #218838; }
        span.rs-ignored-tag { color: #6c757d; background-color: #fff; border: 1px solid #ccc; cursor: default; }
    </style>"""
    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""
    if not races: html_body += "<p>No upcoming races with 3 to 6 runners were found on any of the tracked sources today.</p>"
    else:
        races_by_course, course_order, seen = {}, [], set()
        for race in races:
            races_by_course.setdefault(race['course'], []).append(race)
            if race['course'] not in seen: course_order.append(race['course']); seen.add(race['course'])
        for course in course_order:
            html_body += f'<div class="course-group"><div class="course-header">{course}</div>'
            for race in races_by_course[course]:
                html_body += f'<div class="race-entry"><p class="race-details">Race at {race["time"]} ({race["field_size"]} runners)</p><div class="race-links">'
                html_body += f'<a href="{race["race_url"]}" target="_blank" class="sky-link">Sky Sports</a>'
                if race.get("rs_link"): html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Form</a>'
                elif race['country'] in ['UK', 'IRE']: html_body += '<span class="rs-ignored-tag">Ignored by R&S</span>'
                if race.get("atr_url"): html_body += f'<a href="{race["atr_url"]}" target="_blank" class="atr-link">AtTheRaces</a>'
                if race.get("brisnet_url"): html_body += f'<a href="{race["brisnet_url"]}" target="_blank" class="brisnet-link">Brisnet</a>'
                html_body += '</div></div>'
            html_body += '</div>'
    html_end = f'<div class="footer"><p>Report generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p></div></div></body></html>'
    final_html = html_start + html_body + html_end
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(final_html)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e: print(f"\n❌ Error saving the report: {e}")

def run_mode_B(master_race_list: list[dict]):
    print("\n-- Running Mode B: Restricted Workflow --")
    small_field_races = [r for r in master_race_list if 3 <= r.get('field_size', 0) <= 6]
    print(f"Found {len(small_field_races)} races with 3 to 6 runners.")
    if not small_field_races: generate_mode_B_report([]); return
    print("\n🗞️ Fetching data from Racing & Sports...")
    rs_api_url = "https://www.racingandsports.com.au/todays-racing-json-v2"
    link_fetcher = RacingAndSportsFetcher(rs_api_url)
    json_data = link_fetcher.fetch_data()
    all_rs_meetings = link_fetcher.process_meetings_data(json_data) if json_data else []
    rs_lookup_table = build_rs_lookup_table(all_rs_meetings)
    print("\n🔗 Enriching races with external links...")
    enriched_races = []
    for race in small_field_races:
        # Add R&S links
        if race['country'] in ['UK', 'IRE', 'FR', 'SAF', 'USA', 'AUS', 'URU']:
            if date_iso := race.get('date_iso'):
                if rs_link := find_rs_link(race['course'], date_iso, rs_lookup_table):
                    race['rs_link'] = rs_link
                    print(f"   -> {race['course']} @ {race['time']}: R&S Link FOUND")
        # Add Brisnet and ATR links for all races
        race = generate_external_links(race)
        enriched_races.append(race)
    enriched_races = sort_and_limit_races(enriched_races)
    generate_mode_B_report(enriched_races)

# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================

DATA_SOURCES = [
    {"name": "Sky Sports", "url": "https://www.skysports.com/racing/racecards", "scraper": universal_sky_sports_scan},
    {"name": "Sporting Life", "url": "https://www.sportinglife.com/racing/racecards", "scraper": universal_sporting_life_scan},
]

def main():
    print("=" * 80); print("🚀 Unified Racing Report Generator"); print("=" * 80)
    user_tz = pytz.timezone("America/New_York")
    today_str_yyyymmdd = datetime.now(user_tz).strftime('%Y-%m-%d')
    print(f"📅 Operating on User's Date: {today_str_yyyymmdd}")
    races_dict = {}
    for source in DATA_SOURCES:
        print(f"\n--- Processing source: {source['name']} ---")
        html_content = fetch_page(source['url'])
        if html_content:
            races = source['scraper'](html_content, source['url'], today_str_yyyymmdd)
            print(f"\nProcessing and merging {source['name']} races...")
            for race in races:
                key = (normalize_track_name(race['course']), race['time'])
                if key not in races_dict:
                    races_dict[key] = race
                    print(f"   -> Added new race from {source['name']}: {race['course']} {race['time']}")
                elif (new_size := race.get('field_size')) and new_size > races_dict[key].get('field_size', 0):
                    print(f"   -> Updating field size for {race['course']} {race['time']} to {new_size}")
                    races_dict[key]['field_size'] = new_size
    master_race_list = list(races_dict.values())
    print(f"\nTotal unique races found for today: {len(master_race_list)}")
    if not master_race_list:
        print("\nCould not retrieve any race list for today. Exiting."); return
    if check_attheraces_connectivity():
        run_mode_A(master_race_list)
    else:
        run_mode_B(master_race_list)

if __name__ == "__main__":
    main()
This new file was produced, and I made one small edit to it before sending it.  Line 344 used to start with a period, which obviously is not good.  I changed it from .generate_external_links to generate_external_links and maybe that fixed it.