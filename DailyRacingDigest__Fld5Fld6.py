#!/usr/bin/env python3
"""
Daily Racing Digest & Superfecta Finder (v8.0 - Final)

This script creates a unified report of superfecta betting opportunities by
enriching Sky Sports data with relevant meeting links from Racing & Sports.
It now includes a visual tag for meetings not covered by R&S.

WORKFLOW:
1.  Fetches all meeting data from the Racing & Sports (R&S) API.
2.  Builds an efficient lookup table mapping a (track_name, date) to the
    main R&S form guide link for that entire meeting.
3.  Scrapes the main Sky Sports racecards page to find races with 5-6 runners.
4.  For each qualifying race, it performs a flexible search to find a matching R&S link.
5.  Generates a single, integrated HTML report where each opportunity box
    contains Sky Sports links and either a link to the R&S form or a tag
    indicating the meeting is "Ignored by R&S".
"""

# --- Core Python Libraries ---
import os
import re
import sys
import time
import shutil
import subprocess
import webbrowser
from datetime import datetime
from urllib.parse import urlparse, urljoin

# --- Third-Party Libraries ---
import certifi
import requests
from bs4 import BeautifulSoup
from curl_cffi.requests import Session as CurlCffiSession

# --- Custom Script Import ---
try:
    import RacingAndSports_RacingMonitor_continuous as LinkGenerator
except ImportError:
    print("FATAL ERROR: The file 'RacingAndSports_RacingMonitor_continuous.py' could not be found.")
    print("Please ensure both .py files are in the same directory.")
    sys.exit(1)

# --- Suppress SSL Warnings ---
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# ==============================================================================
# UNIVERSAL HELPER & FETCHING LOGIC
# ==============================================================================

def fetch_page_source(url):
    """Attempts to fetch HTML source using multiple methods."""
    print(f"-> Fetching source for: {url}")
    source_code = try_curl_cffi(url) or try_requests_with_variations(url) or try_subprocess_curl(url)
    if source_code:
        print("   ✅ Source code retrieved.")
    else:
        print(f"   ❌ All methods failed for this URL.")
    return source_code

def try_curl_cffi(url):
    try:
        session = CurlCffiSession(impersonate="chrome120", timeout=20)
        response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=certifi.where())
        response.raise_for_status()
        return response.text
    except Exception: return None

def try_requests_with_variations(url):
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=15)
        if response.status_code == 200 and len(response.text) > 500: return response.text
    except Exception: return None
    return None

def try_subprocess_curl(url):
    if not shutil.which("curl"): return None
    try:
        result = subprocess.run(['curl', '-s', '-L', '-A', 'Mozilla/5.0', url], capture_output=True, text=True, timeout=30, check=True)
        return result.stdout
    except Exception: return None

def normalize_track_name(name):
    """Prepares a track name for reliable matching by lowercasing and stripping common suffixes."""
    return name.lower().strip().replace('(july)', '').replace('(aw)', '').replace('acton', '').strip()

# ==============================================================================
# DATA PROCESSING AND MATCHING LOGIC
# ==============================================================================

def build_rs_lookup_table(rs_meetings):
    """
    Processes R&S data into a lookup table.
    Key: (normalized_track_name, date_YYYY-MM-DD), Value: R&S meeting link.
    """
    lookup = {}
    if not rs_meetings: return lookup
    
    print("...Building Racing & Sports lookup table for matching...")
    for meeting in rs_meetings:
        course = meeting.get('course')
        link = meeting.get('pdf_link')
        if not course or not link: continue
        
        match = re.search(r'/(\d{4}-\d{2}-\d{2})', link)
        if not match: continue
            
        date_str = match.group(1)
        normalized_course = normalize_track_name(course)
        lookup[(normalized_course, date_str)] = link
        
    print(f"   ✅ Lookup table created with {len(lookup)} entries.")
    return lookup

def find_rs_meeting_link(sky_track, sky_date_ddmmyyyy, lookup_table):
    """
    Finds a matching R&S meeting link using flexible, two-way substring matching.
    """
    try:
        date_obj = datetime.strptime(sky_date_ddmmyyyy, '%d-%m-%Y')
        normalized_date = date_obj.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

    normalized_sky_track = normalize_track_name(sky_track)

    for (rs_track, rs_date), rs_link in lookup_table.items():
        if rs_date == normalized_date:
            if normalized_sky_track in rs_track or rs_track in normalized_sky_track:
                return rs_link
                
    return None

def parse_sky_sports_data(html_content, base_url, rs_lookup_table):
    """
    Parses Sky Sports HTML, filters for 5-6 runner races, and enriches
    with matching R&S meeting links.
    """
    filtered_races = []
    if not html_content: return filtered_races

    soup = BeautifulSoup(html_content, 'html.parser')
    
    def parse_sky_url_for_info(url):
        try:
            path_parts = urlparse(url).path.strip('/').split('/')
            if 'racecards' in path_parts:
                idx = path_parts.index('racecards')
                track = path_parts[idx + 1].replace('-', ' ').title()
                date = path_parts[idx + 2]
                return track, date
        except Exception: return None, None
    
    event_containers = soup.find_all('div', class_='sdc-site-racing-meetings__event')
    
    for container in event_containers:
        racecard_tag = container.find('a', class_='sdc-site-racing-meetings__event-link')
        race_details_span = container.find('span', class_='sdc-site-racing-meetings__event-details')
        
        if not racecard_tag or not race_details_span: continue
            
        details_text = race_details_span.get_text(strip=True)
        runner_count_match = re.search(r'(\d+)\s+runners?', details_text, re.IGNORECASE)
        if not runner_count_match or not (5 <= int(runner_count_match.group(1)) <= 6): continue

        runner_count = int(runner_count_match.group(1))
        racecard_url = urljoin(base_url, racecard_tag.get('href'))
        track, date = parse_sky_url_for_info(racecard_url)
        
        if not track or not date: continue

        race_name_span = container.find('span', class_='sdc-site-racing-meetings__event-name')
        display_text = f"[{track}] {race_name_span.get_text(strip=True)} ({details_text})"
        
        result_tag = container.find('a', class_='sdc-site-racing-meetings__event-result')
        result_url = urljoin(base_url, result_tag.get('href')) if result_tag else None
        
        rs_link = find_rs_meeting_link(track, date, rs_lookup_table)
        
        print(f"   ⭐ FOUND MATCH: {track} ({runner_count} runners) -> R&S Link Found: {'Yes' if rs_link else 'No'}")
        
        filtered_races.append({
            "display_text": display_text, "racecard_url": racecard_url,
            "result_url": result_url, "runner_count": runner_count, "rs_link": rs_link
        })
    return filtered_races

# ==============================================================================
# HTML REPORT GENERATION
# ==============================================================================

STRATEGIES = {
    5: "[Superfecta Key: Box Top 4 / Wheel 1st]",
    6: "[Superfecta Key: Box Top 4 / Wheel 1st & 2nd]",
}

def generate_final_report(races, page_title):
    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f4f4f9; color: #333; margin: 20px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        h1 { color: #0056b3; text-align: center; border-bottom: 3px solid #0056b3; padding-bottom: 10px; }
        h2.runner-count-header { font-size: 1.4em; color: #fff; background-color: #343a40; padding: 10px 15px; margin: 30px 0 15px; border-radius: 5px; }
        .race-entry { border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fafafa; }
        .race-details { font-weight: bold; font-size: 1.1em; color: #333; margin-bottom: 10px; }
        .race-links a, .race-links span { display: inline-block; text-decoration: none; padding: 8px 15px; border-radius: 4px; margin: 5px 10px 5px 0; font-weight: bold; }
        a.racecard-link { background-color: #007bff; color: white; } a.racecard-link:hover { background-color: #0056b3; }
        a.result-link { background-color: #28a745; color: white; } a.result-link:hover { background-color: #218838; }
        /* --- UNIFIED STYLES for R&S tags --- */
        a.rs-link, span.rs-ignored-tag { min-width: 160px; text-align: center; }
        a.rs-link { background-color: #dc3545; color: white; } a.rs-link:hover { background-color: #c82333; }
        span.rs-ignored-tag { color: #dc3545; background-color: #fff; border: 1px solid #dc3545; cursor: default; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""
    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{page_title}</title>{html_css}</head><body><div class="container"><h1>{page_title}</h1>'
    html_body = ""
    if not races:
        html_body += "<p>No races with 5 or 6 runners were found on Sky Sports today.</p>"
    else:
        last_runner_count = None
        for race in races:
            if race['runner_count'] != last_runner_count:
                strategy_text = STRATEGIES.get(race['runner_count'], "")
                html_body += f'<h2 class="runner-count-header">{race["runner_count"]} Runner Races &nbsp;&nbsp; <small style="font-weight:normal;">{strategy_text}</small></h2>'
                last_runner_count = race['runner_count']
            
            html_body += f'<div class="race-entry"><p class="race-details">{race["display_text"]}</p><div class="race-links">'
            html_body += f'<a href="{race["racecard_url"]}" target="_blank" class="racecard-link">Sky Sports Racecard</a>'
            
            if race["rs_link"]:
                html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Meeting Form</a>'
            else:
                html_body += '<span class="rs-ignored-tag">Ignored by R&S</span>'

            if race["result_url"]:
                html_body += f'<a href="{race["result_url"]}" target="_blank" class="result-link">Sky Sports Result</a>'
            
            html_body += '</div></div>'

    timestamp = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
    html_end = f'<div class="footer"><p>Report generated on {timestamp}</p></div></div></body></html>'
    return html_start + html_body + html_end

# ==============================================================================
# MAIN ORCHESTRATION LOGIC
# ==============================================================================

def main():
    start_time = time.time()
    print("=" * 60, "\n🚀 Daily Racing Digest & Superfecta Finder v8.0 (Final)\n" + "=" * 60)

    print("\n--- STEP 1: Processing Racing & Sports for Meeting Data ---")
    rs_api_url = "https://www.racingandsports.com.au/todays-racing-json-v2"
    link_fetcher = LinkGenerator.RacingAndSportsFetcher(rs_api_url)
    json_data = link_fetcher.fetch_data()
    all_rs_meetings = link_fetcher.process_meetings_data(json_data) if json_data else []
    rs_lookup_table = build_rs_lookup_table(all_rs_meetings)

    print("\n--- STEP 2: Processing Sky Sports & Matching Data ---")
    SKYSPORTS_URL = "https://www.skysports.com/racing/racecards"
    sky_source = fetch_page_source(SKYSPORTS_URL)
    superfecta_races = parse_sky_sports_data(sky_source, SKYSPORTS_URL, rs_lookup_table) if sky_source else []
    superfecta_races.sort(key=lambda x: x['runner_count'])

    print("\n--- STEP 3: Generating Final Integrated Report ---")
    page_title = f"Superfecta Opportunities ({len(superfecta_races)} found)"
    final_html = generate_final_report(superfecta_races, page_title)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"Superfecta_Opportunities_Report_{timestamp}.htm"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(final_html)
        absolute_path = os.path.abspath(filename)
        print(f"\n🎉 SUCCESS! Report generated: {absolute_path}")
        webbrowser.open(f'file://{absolute_path}')
    except Exception as e:
        print(f"\n❌ Error saving the final report: {e}")

    print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
