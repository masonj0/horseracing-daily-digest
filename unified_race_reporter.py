#!/usr/bin/env python3
"""
Unified Racing Report Generator (v1.0)

This script generates one of two valuable horse racing reports based on the
network environment. It scrapes race data from Sky Sports and, if possible,
enriches it with live odds from AtTheRaces. Otherwise, it provides a list of
races with links to external form guides.
"""

# --- Core Python Libraries ---
import os
import re
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, urlparse

# --- Third-Party Libraries ---
import requests
from bs4 import BeautifulSoup
from curl_cffi.requests import Session as CurlCffiSession

# --- Suppress SSL Warnings ---
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def robust_fetch(url: str) -> str:
    """
    Attempts to fetch a URL using two methods: a standard request and a
    browser-impersonating request via curl_cffi.
    """
    # Attempt 1: Standard "robot" request
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20, verify=False)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        print(f"   -> Standard request to {url} failed. Trying browser impersonation...")

    # Attempt 2: "Browser" impersonation request
    try:
        session = CurlCffiSession(impersonate="chrome110")
        response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        raise ConnectionError(f"All fetch methods failed for {url}") from e

def normalize_track_name(name: str) -> str:
    """Prepares a track name for reliable matching by lowercasing and stripping common suffixes."""
    if not isinstance(name, str):
        return ""
    return name.lower().strip().replace('(july)', '').replace('(aw)', '').replace('acton', '').strip()

def convert_utc_to_eastern(utc_dt_str: str) -> str:
    """
    Converts a UTC datetime string into a user-friendly US/Eastern time string.
    Returns an empty string if the input format is not a full UTC datetime,
    signaling the caller to use a fallback.
    """
    if not utc_dt_str or ('+' not in utc_dt_str and 'Z' not in utc_dt_str):
        # This is likely a naive time like "2024-08-10 14:00", which we cannot accurately convert.
        return ""

    try:
        # Parse the full UTC datetime string
        utc_dt = datetime.fromisoformat(utc_dt_str.replace('Z', '+00:00'))

        # Convert to US/Eastern timezone
        eastern_tz = ZoneInfo("America/New_York")
        eastern_dt = utc_dt.astimezone(eastern_tz)

        # Format into a friendly string, e.g., "17h30"
        return eastern_dt.strftime('%Hh%M')

    except (ValueError, TypeError):
        # Handle potential parsing errors
        return ""

# ==============================================================================
# STEP 1: UNIVERSAL SCAN
# ==============================================================================

def universal_atr_scan(regions: list[str]):
    """
    Primary universal scan. Fetches race data and odds from the AtTheRaces AJAX endpoint.
    This serves as our main data source and environmental check in one.
    If it fails, it raises a ConnectionError.
    """
    print("\n🛰️ Performing Universal Scan from AtTheRaces...")
    today_str_url = datetime.now().strftime('%Y-%m-%d')
    today_str_atr = datetime.now().strftime('%Y%m%d')

    master_race_list = []

    for region in regions:
        url = f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{today_str_atr}"
        print(f"-> Querying {region.upper()} races from: {url}")

        try:
            html_content = robust_fetch(url)
        except ConnectionError as e:
            print(f"   ❌ CRITICAL: Could not fetch data from AtTheRaces for region {region}. {e}")
            # Raise an exception to be caught by the main orchestrator.
            raise ConnectionError("Failed to connect to AtTheRaces.") from e

        soup = BeautifulSoup(html_content, 'html.parser')
        race_captions = soup.find_all('caption', string=re.compile(r"^\d{2}:\d{2}"))

        for caption in race_captions:
            race_name_full = caption.get_text(strip=True)
            race_time_match = re.match(r"(\d{2}:\d{2})", race_name_full)
            race_time = race_time_match.group(1) if race_time_match else None
            if not race_time: continue

            course_header = caption.find_parent('div', class_='panel').find('h2')
            course_name = course_header.get_text(strip=True) if course_header else None
            if not course_name: continue

            race_table = caption.find_next_sibling('table')
            if not race_table: continue

            horses = []
            for row in race_table.find('tbody').find_all('tr'):
                cells = row.find_all('td')
                if not cells: continue
                horse_name = cells[0].get_text(strip=True)
                current_odds_str = cells[1].get_text(strip=True)
                horses.append({
                    'name': horse_name,
                    'odds_str': current_odds_str,
                    'odds_float': convert_odds_to_float(current_odds_str)
                })

            if not horses: continue
            horses.sort(key=lambda x: x['odds_float'])

            course_slug = course_name.replace(' ', '-').lower()
            time_slug = race_time.replace(':', '')
            race_url = f"https://www.attheraces.com/racecard/{course_slug}/{today_str_url}/{time_slug}"

            # Combine date and time to create a naive datetime object for sorting
            # We don't know the original timezone, so this is the best we can do.
            full_datetime_str = f"{today_str_url} {race_time}"

            master_race_list.append({
                'course': course_name,
                'time': race_time,
                'datetime_utc': full_datetime_str, # Store the combined datetime string
                'field_size': len(horses),
                'country': region.upper(),
                'race_url': race_url,
                'favorite': horses[0] if len(horses) > 0 else None,
                'second_favorite': horses[1] if len(horses) > 1 else None,
            })

    print(f"✅ ATR Universal Scan complete. Found {len(master_race_list)} races globally.")
    return master_race_list


# ==============================================================================
# UNIFIED REPORT GENERATION
# ==============================================================================

def generate_unified_report(races: list[dict]):
    """
    Generates a single, unified HTML report that intelligently displays all
    available data for each race.
    """
    # --- Part 1: Data Enrichment (Racing & Sports Links) ---
    print("\n🗞️ Fetching data from Racing & Sports to find matching form guides...")
    rs_api_url = "https://www.racingandsports.com.au/todays-racing-json-v2"
    # Note: The R&S Fetcher and its helpers are now defined inside this function
    # to keep them scoped, as they are only used here.
    class RacingAndSportsFetcher:
        def __init__(self, api_url):
            self.api_url = api_url
            self.session = requests.Session()
            self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        def fetch_data(self):
            try:
                # Use robust_fetch for this call as well
                json_text = robust_fetch(self.api_url)
                return json.loads(json_text)
            except Exception: return None
        def process_meetings(self, data):
            if not isinstance(data, list): return []
            meetings = []
            for discipline in data:
                for country in discipline.get("Countries", []):
                    for meeting in country.get("Meetings", []):
                        if meeting.get("Course") and (meeting.get("PDFUrl") or meeting.get("PreMeetingUrl")):
                            meetings.append({'course': meeting["Course"], 'link': meeting.get("PDFUrl") or meeting.get("PreMeetingUrl")})
            return meetings

    link_fetcher = RacingAndSportsFetcher(rs_api_url)
    json_data = link_fetcher.fetch_data()
    all_rs_meetings = link_fetcher.process_meetings(json_data) if json_data else []

    # Build a lookup table from the R&S data
    rs_lookup_table = {}
    for meeting in all_rs_meetings:
        link = meeting.get('link')
        match = re.search(r'/(\d{4}-\d{2}-\d{2})', link)
        if match:
            rs_lookup_table[(normalize_track_name(meeting['course']), match.group(1))] = link
    print(f"   ✅ R&S lookup table created with {len(rs_lookup_table)} entries.")

    # Enrich the main race list with R&S links
    print("\n🔗 Attempting to match all races with R&S links...")
    for race in races:
        try:
            date_str_iso = re.search(r'(\d{4}-\d{2}-\d{2})', race['race_url']).group(1)
            key = (normalize_track_name(race['course']), date_str_iso)
            if key in rs_lookup_table:
                race['rs_link'] = rs_lookup_table[key]
        except (AttributeError, KeyError):
            continue # Don't add a link if we can't find one

    # --- Part 2: HTML Report Generation ---
    print("\n📄 Generating unified HTML report...")
    title = "Global Racing Digest"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"Global_Racing_Digest_{timestamp}.html"

    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f8f9fa; color: #212529; margin: 20px; }
        .container { max-width: 1000px; margin: auto; background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
        h1 { color: #343a40; text-align: center; border-bottom: 2px solid #dee2e6; padding-bottom: 15px; }
        .course-group { margin-bottom: 30px; }
        .course-header { font-size: 1.8em; font-weight: bold; color: #495057; padding-bottom: 10px; border-bottom: 1px solid #e9ecef; margin-bottom: 15px; }
        .race-entry { border: 1px solid #dee2e6; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fff; }
        .race-entry.analytical-match { border-left: 5px solid #28a745; }
        .race-details { font-weight: bold; font-size: 1.1em; color: #343a40; margin-bottom: 10px; }
        .race-links a, .race-links span { display: inline-block; text-decoration: none; padding: 8px 15px; border-radius: 4px; margin: 5px 10px 5px 0; font-weight: bold; min-width: 160px; text-align: center; }
        a.atr-link { background-color: #007bff; color: white; } a.atr-link:hover { background-color: #0056b3; }
        a.rs-link { background-color: #dc3545; color: white; } a.rs-link:hover { background-color: #c82333; }
        span.rs-ignored-tag { color: #6c757d; background-color: #fff; border: 1px solid #ccc; cursor: default; }
        .odds-info { background-color: #f8f9fa; border: 1px solid #e9ecef; padding: 10px; margin-top: 10px; border-radius: 4px; }
        .match-highlight { color: #28a745; font-weight: bold; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #6c757d; }
    </style>"""

    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""

    if not races:
        html_body += "<h3>No races found from any data source.</h3>"
    else:
        races_by_course = {}
        for race in races:
            races_by_course.setdefault(f"{race['course']} ({race['country']})", []).append(race)

        for course, course_races in sorted(races_by_course.items()):
            html_body += f'<div class="course-group"><div class="course-header">{course}</div>'
            for race in sorted(course_races, key=lambda r: r.get('datetime_utc', r.get('time', ''))):
                display_time = convert_utc_to_eastern(race.get('datetime_utc')) or f"{race.get('time', 'N/A').replace(':', 'h')} (Timezone Unknown)"

                # Check for analytical match
                is_match = False
                if race.get('favorite'):
                    field_size_ok = race['field_size'] < 7
                    fav_odds_ok = race['favorite']['odds_float'] >= 1.0
                    sec_fav = race.get('second_favorite')
                    sec_fav_odds_ok = sec_fav and sec_fav['odds_float'] >= 3.0
                    if field_size_ok and fav_odds_ok and sec_fav_odds_ok:
                        is_match = True

                match_class = "analytical-match" if is_match else ""
                html_body += f'<div class="race-entry {match_class}">'
                html_body += f'<p class="race-details">Race at {display_time} ({race["field_size"]} runners)</p>'

                # Render based on available data
                if race.get('favorite'):
                    fav = race['favorite']
                    sec_fav = race.get('second_favorite')
                    html_body += '<div class="odds-info">'
                    if is_match:
                        html_body += '<p class="match-highlight">⭐ Analytical Match Found!</p>'
                    html_body += f"<b>Favorite:</b> {fav['name']} (<b>{fav['odds_str']}</b>)<br>"
                    if sec_fav:
                        html_body += f"<b>2nd Favorite:</b> {sec_fav['name']} (<b>{sec_fav['odds_str']}</b>)"
                    html_body += '</div>'
                else:
                    html_body += '<div class="race-links">'
                    html_body += f'<a href="{race["race_url"]}" target="_blank" class="atr-link">Racecard Link</a>'
                    if race.get("rs_link"):
                        html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Form</a>'
                    else:
                        html_body += '<span class="rs-ignored-tag">No R&S Form</span>'
                    html_body += '</div>'

                html_body += '</div>'
            html_body += '</div>'

    report_time = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
    html_end = f'<div class="footer"><p>Report generated on {report_time}</p></div></div></body></html>'
    final_html = html_start + html_body + html_end

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(final_html)
        print(f"\n🎉 SUCCESS! Unified report generated: {os.path.abspath(filename)}")
    except Exception as e:
        print(f"\n❌ Error saving the report: {e}")

# ==============================================================================
# FALLBACK DATA SOURCE #1: ODDSCHECKER
# ==============================================================================
def scrape_oddschecker():
    """
    FALLBACK #1: Scrapes oddschecker.com for global race data.
    This is used if the primary AtTheRaces API fails.
    """
    print("\n scraping oddschecker.com for global race data...")
    base_url = "https://www.oddschecker.com"
    race_list_url = f"{base_url}/horse-racing"

    try:
        html_content = robust_fetch(race_list_url)
        soup = BeautifulSoup(html_content, 'html.parser')
    except ConnectionError as e:
        print(f"   ❌ Failed to fetch oddschecker race list: {e}")
        raise ConnectionError("Could not connect to oddschecker.com") from e

    race_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href and href.startswith('/horse-racing/') and re.search(r'/\d{4}-\d{2}-\d{2}-', href):
            race_links.append(f"{base_url}{href}")

    race_links = sorted(list(set(race_links)))
    print(f"Found {len(race_links)} race links to scrape on oddschecker.")

    master_race_list = []
    for race_url in race_links[:20]: # Limit requests to avoid being blocked during testing
        try:
            print(f"   -> Scraping race: {race_url}")
            race_page_html = robust_fetch(race_url)
            race_soup = BeautifulSoup(race_page_html, 'html.parser')

            # Extract course and time from URL
            parts = race_url.split('/')
            course = parts[-2].split('-')[-1].replace('-', ' ').title()
            race_time = parts[-1]

            # Extract country
            country_tag = race_soup.find('img', {'class': 'race-header-country-flag'})
            country = country_tag['alt'] if country_tag else 'Unknown'

            # Extract field size
            starters_tag = race_soup.find('span', string=re.compile(r'Starters'))
            field_size = int(re.search(r'\d+', starters_tag.text).group()) if starters_tag else 0
            if field_size == 0: continue # Skip if we can't find field size

            # This page doesn't have full UTC, so we create a naive string
            date_str = re.search(r'(\d{4}-\d{2}-\d{2})', race_url).group(1)
            datetime_utc_str = f"{date_str} {race_time}"

            # Scrape horses and odds
            horses = []
            horse_rows = race_soup.find_all('tr', {'class': 'diff-row ev-expand-btn'})
            for row in horse_rows:
                horse_name_tag = row.find('p', {'class': 'race-card-horse-name'})
                odds_tag = row.find('p', {'class': 'race-card-odds'})
                if horse_name_tag and odds_tag:
                    horses.append({
                        'name': horse_name_tag.text.strip(),
                        'odds_str': odds_tag.text.strip(),
                        'odds_float': convert_odds_to_float(odds_tag.text.strip())
                    })

            if not horses: continue
            horses.sort(key=lambda x: x['odds_float'])

            master_race_list.append({
                'course': course,
                'time': race_time,
                'datetime_utc': datetime_utc_str,
                'field_size': field_size,
                'country': country,
                'race_url': race_url,
                'favorite': horses[0] if len(horses) > 0 else None,
                'second_favorite': horses[1] if len(horses) > 1 else None,
            })
        except Exception as e:
            print(f"   ⚠️ Could not scrape race URL {race_url}: {e}")
            continue

    print(f"✅ Oddschecker scrape complete. Found data for {len(master_race_list)} races.")
    return master_race_list

# ==============================================================================
# FALLBACK DATA SOURCE #2 (for Restricted Mode)
# ==============================================================================
def fetch_races_from_rpb2b_api():
    """
    FALLBACK SCAN: Fetches daily racecards from the rpb2b.com JSON API.
    This is used only when the primary AtTheRaces source is unavailable.
    NOTE: This API endpoint appears to be for North American (USA/CAN) races only.
    """
    print("\n🔍 Fetching fallback race data from rpb2b.com API...")
    today_str = datetime.now().strftime('%Y-%m-%d')
    api_url = f"https://backend-us-racecards.widget.rpb2b.com/v2/racecards/daily/{today_str}"

    print(f"-> Querying API: {api_url}")
    try:
        api_text = robust_fetch(api_url)
        api_data = json.loads(api_text)
        print("   ✅ Success.")
    except (ConnectionError, json.JSONDecodeError) as e:
        print(f"   ❌ Failed to fetch from fallback API: {e}")
        return []

    fallback_race_list = []
    for meeting in api_data:
        course_name = meeting.get('name')
        country_code = meeting.get('countryCode')
        if not course_name or not country_code: continue

        course_slug = course_name.lower().replace(' ', '-')
        sky_meeting_url = f"https://www.skysports.com/racing/racecards/{course_slug}/{today_str}"

        for race in meeting.get('races', []):
            utc_datetime_str = race.get('datetimeUtc')
            num_runners = race.get('numberOfRunners')
            if not utc_datetime_str or num_runners is None: continue

            try:
                race_datetime_utc = datetime.fromisoformat(utc_datetime_str.replace('Z', '+00:00'))
                race_time_str = race_datetime_utc.strftime('%H:%M')
            except ValueError: continue

            fallback_race_list.append({
                'course': course_name,
                'time': race_time_str,
                'datetime_utc': utc_datetime_str,
                'field_size': num_runners,
                'race_url': sky_meeting_url,
                'country': country_code,
                'favorite': None,
                'second_favorite': None,
            })
    return fallback_race_list

# ==============================================================================
# DATA FETCHING ORCHESTRATION
# ==============================================================================
def get_best_available_races():
    """
    Attempts to fetch race data from the best available source in a specific order.
    Implements a graceful fallback chain.
    """
    # --- Attempt 1: Primary API (AtTheRaces) ---
    try:
        print("\n--- Attempting Primary Source: AtTheRaces API ---")
        atr_regions = ['uk', 'ireland', 'usa', 'france', 'saf', 'aus']
        master_race_list = universal_atr_scan(atr_regions)
        print("\n✅ Primary source successful. Using rich data from AtTheRaces.")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to fallback source #1.")

    # --- Attempt 2: Fallback Scraper (Oddschecker) ---
    try:
        print("\n--- Attempting Fallback Source #1: Oddschecker Scraper ---")
        master_race_list = scrape_oddschecker()
        if not master_race_list: raise ConnectionError("Oddschecker returned no data.")
        print("\n✅ Oddschecker scrape successful. Using global data (no live odds).")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to fallback source #2.")

    # --- Attempt 3: Fallback API (rpb2b.com) ---
    try:
        print("\n--- Attempting Fallback Source #2: rpb2b.com API ---")
        master_race_list = fetch_races_from_rpb2b_api()
        if not master_race_list: raise ConnectionError("rpb2b.com API returned no data.")
        print("\n✅ rpb2b.com API successful. Using NA-only data.")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to final fallback source.")

    # --- Attempt 4: Final Fallback Scraper (Sky Sports) ---
    try:
        print("\n--- Attempting Final Fallback Source #3: Sky Sports Scraper ---")
        master_race_list = scrape_sky_sports()
        if not master_race_list: raise ConnectionError("Sky Sports scrape returned no data.")
        print("\n✅ Sky Sports scrape successful. Using global data (no live odds).")
        return master_race_list
    except Exception as e:
        print(f"\n❌ All data sources failed. Could not retrieve any data. {e}")

    return [] # Return an empty list if all sources fail

# ==============================================================================
# FALLBACK DATA SOURCE #3: SKY SPORTS SCRAPER
# ==============================================================================
def scrape_sky_sports():
    """
    FALLBACK #3: Scrapes skysports.com for global race data.
    This is the final fallback if all other data sources fail.
    """
    print("\n scraping skysports.com for global race data...")
    base_url = "https://www.skysports.com"
    race_list_url = f"{base_url}/racing/racecards"

    try:
        html_content = robust_fetch(race_list_url)
        soup = BeautifulSoup(html_content, 'html.parser')
    except ConnectionError as e:
        print(f"   ❌ Failed to fetch skysports.com: {e}")
        raise ConnectionError("Could not connect to skysports.com") from e

    all_races = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    meeting_containers = soup.find_all('div', class_='sdc-site-racing-meetings-group')
    for meeting_container in meeting_containers:
        country_header = meeting_container.find('h2', class_='sdc-site-racing-meetings__title')
        country = country_header.get_text(strip=True) if country_header else "Unknown"

        event_containers = meeting_container.find_all('div', class_='sdc-site-racing-meetings__event')
        for container in event_containers:
            racecard_tag = container.find('a', class_='sdc-site-racing-meetings__event-link')
            race_details_span = container.find('span', class_='sdc-site-racing-meetings__event-details')
            if not racecard_tag or not race_details_span: continue

            details_text = race_details_span.get_text(strip=True)
            runner_count_match = re.search(r'(\d+)\s+runners?', details_text, re.IGNORECASE)
            if not runner_count_match: continue
            field_size = int(runner_count_match.group(1))

            race_url = urljoin(base_url, racecard_tag.get('href'))

            try:
                path_parts = urlparse(race_url).path.strip('/').split('/')
                course = path_parts[path_parts.index('racecards') + 1].replace('-', ' ').title()
            except (ValueError, IndexError):
                continue

            race_name_span = container.find('span', class_='sdc-site-racing-meetings__event-name')
            time_match = re.search(r'(\d{1,2}:\d{2})', race_name_span.get_text(strip=True))
            race_time = time_match.group(1) if time_match else "N/A"

            all_races.append({
                'course': course,
                'time': race_time,
                'datetime_utc': f"{today_str} {race_time}",
                'field_size': field_size,
                'race_url': race_url,
                'country': country,
                'favorite': None,
                'second_favorite': None,
            })

    print(f"✅ Sky Sports scrape complete. Found data for {len(all_races)} races.")
    return all_races

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """
    Main execution block.
    Fetches the best available race data and generates the unified report.
    """
    print("=" * 80)
    print("🚀 Unified Racing Report Generator")
    print("=" * 80)

    # Get the best possible list of races using the fallback chain
    master_race_list = get_best_available_races()

    # Generate the single, unified report
    if master_race_list:
        generate_unified_report(master_race_list)
    else:
        print("\n❌ Could not retrieve data from any source. Exiting.")

    print("\n🏁 Script finished.")


if __name__ == "__main__":
    main()
