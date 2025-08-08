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
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin

# --- Third-Party Libraries ---
import requests
import pytz
from bs4 import BeautifulSoup

# --- Suppress SSL Warnings ---
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def normalize_track_name(name: str) -> str:
    """Prepares a track name for reliable matching by lowercasing and stripping common suffixes."""
    if not isinstance(name, str):
        return ""
    return name.lower().strip().replace('(july)', '').replace('(aw)', '').replace('acton', '').strip()

def fetch_page(url: str):
    """Fetches the HTML content of a page using requests."""
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
    """
    Filters for upcoming races, sorts them chronologically, and limits the list.
    Assumes that the `datetime_utc` key has already been calculated.
    """
    print(f"\n⏳ Filtering for upcoming races, sorting, and limiting...")
    now_utc = datetime.now(pytz.utc)

    # Filter for races that have a valid UTC datetime and are in the future
    future_races = [
        race for race in races
        if race.get('datetime_utc') and race['datetime_utc'] > now_utc
    ]

    # Sort the filtered races by their UTC datetime
    future_races.sort(key=lambda r: r['datetime_utc'])

    # Limit the number of races
    limited_races = future_races[:limit]

    print(f"   -> Found {len(future_races)} upcoming races.")
    print(f"   -> Limiting to a maximum of {limit} races.")
    print(f"✅ Filtering and limiting complete. {len(limited_races)} races remain.")

    return limited_races

# ==============================================================================
# STEP 1: UNIVERSAL SCAN
# ==============================================================================

def universal_sky_sports_scan(html_content: str, base_url: str):
    """
    (Corrected Final Version) Scrapes Sky Sports racecards. This version is
    based on the correct HTML structure and uses the ground truth that all
    times are presented in UK time.
    """
    if not html_content:
        print("❌ HTML content is empty. Cannot perform the scan.")
        return []

    print("\n🔍 Starting Universal Scan of Sky Sports...")
    soup = BeautifulSoup(html_content, 'html.parser')
    all_races = []

    # Ground truth: All times on Sky Sports are UK time.
    source_tz = pytz.timezone('Europe/London')

    def get_country_code(meeting_title: str) -> str:
        """Parses a meeting title (e.g., 'Tipperary (IRE)') to get a country code for display."""
        match = re.search(r'\((\w+)\)$', meeting_title)
        return match.group(1).upper() if match else "UK"

    def parse_sky_url_for_info(url):
        """Extract track name and date from Sky Sports URL."""
        try:
            path_parts = urlparse(url).path.strip('/').split('/')
            if 'racecards' in path_parts:
                idx = path_parts.index('racecards')
                track = path_parts[idx + 1].replace('-', ' ').title()
                date = path_parts[idx + 2] if len(path_parts) > idx + 2 else None
                return track, date
        except (ValueError, IndexError):
            return None, None
        return None, None

    # Each meeting is wrapped in a "concertina-block"
    for meeting_block in soup.find_all('div', class_='sdc-site-concertina-block'):
        title_tag = meeting_block.find('h3', class_='sdc-site-concertina-block__title')
        if not title_tag:
            continue

        meeting_title = title_tag.get_text(strip=True)
        country_code = get_country_code(meeting_title)

        # Find all race events within this meeting's block
        events_container = meeting_block.find('div', class_='sdc-site-racing-meetings__events')
        if not events_container:
            continue

        for container in events_container.find_all('div', class_='sdc-site-racing-meetings__event'):
            racecard_tag = container.find('a', class_='sdc-site-racing-meetings__event-link')
            race_details_span = container.find('span', class_='sdc-site-racing-meetings__event-details')

            if not racecard_tag or not race_details_span:
                continue

            runner_match = re.search(r'(\d+)\s+runners?', race_details_span.get_text(strip=True), re.IGNORECASE)
            field_size = int(runner_match.group(1)) if runner_match else 0

            race_url = urljoin(base_url, racecard_tag.get('href'))
            course, date_str = parse_sky_url_for_info(race_url)
            if not course or not date_str:
                continue

            time_span = container.find('span', class_='sdc-site-racing-meetings__event-time')
            race_time = time_span.get_text(strip=True) if time_span else "N/A"

            datetime_utc = None
            if race_time != "N/A" and date_str:
                try:
                    # The date format from the URL is DD-MM-YYYY
                    naive_dt = datetime.strptime(f"{date_str} {race_time}", '%d-%m-%Y %H:%M')
                    # Localize all times to London, then convert to UTC
                    datetime_utc = source_tz.localize(naive_dt).astimezone(pytz.utc)
                except (ValueError, KeyError):
                    pass

            all_races.append({
                'course': course, 'time': race_time, 'field_size': field_size,
                'race_url': race_url, 'country': country_code, 'date_iso': date_str,
                'datetime_utc': datetime_utc
            })
            print(f"   -> Found: {course} ({country_code}) at {race_time} [Europe/London]")

    print(f"✅ Universal Scan complete. Found {len(all_races)} races in total.")
    return all_races

# ==============================================================================
# STEP 2: ENVIRONMENTAL CHECK
# ==============================================================================

def check_attheraces_connectivity(url="https://www.attheraces.com/"):
    """
    Checks if the AtTheRaces website is reachable to determine the operating mode.
    """
    print("\n🌐 Performing Environmental Check...")
    print(f"-> Pinging: {url}")
    try:
        # Use a HEAD request for efficiency as we only need the status, not the content.
        response = requests.head(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=10)
        response.raise_for_status()
        if response.status_code == 200:
            print("   ✅ Success! Network is UNRESTRICTED.")
            return True
    except requests.exceptions.RequestException:
        pass  # We expect this to fail in a restricted environment.

    print("   ⚠️ AtTheRaces is unreachable. Network is RESTRICTED.")
    return False

# ==============================================================================
# MODE A: UNRESTRICTED WORKFLOW
# ==============================================================================

def convert_odds_to_float(odds_str: str) -> float:
    """
    Converts a fractional odds string (e.g., '5/2', 'EVS') to a float.
    Returns a high number for invalid odds to ensure they are sorted last.
    """
    if not isinstance(odds_str, str):
        return 9999.0

    odds_str = odds_str.strip().upper()

    if 'SP' in odds_str:
        return 9999.0
    if odds_str == 'EVS':
        return 1.0

    if '/' in odds_str:
        try:
            numerator, denominator = map(float, odds_str.split('/'))
            if denominator == 0:
                return 9999.0
            return numerator / denominator
        except (ValueError, IndexError):
            return 9999.0

    try:
        return float(odds_str)
    except ValueError:
        return 9999.0

def fetch_atr_odds_data(regions: list[str]) -> dict:
    """
    Fetches and parses all races for given regions from the ATR AJAX endpoint.
    Returns a lookup dictionary mapping (normalized_course, time) to race data.
    """
    print("\n📡 Fetching Live Odds from AtTheRaces...")
    today_str = datetime.now().strftime('%Y%m%d')
    atr_races_lookup = {}

    for region in regions:
        url = f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{today_str}"
        print(f"-> Querying {region.upper()} races from: {url}")

        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️ Could not fetch data for {region}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        race_captions = soup.find_all('caption', string=re.compile(r"^\d{2}:\d{2}"))

        for caption in race_captions:
            race_name_full = caption.get_text(strip=True)
            race_time_match = re.match(r"(\d{2}:\d{2})", race_name_full)
            race_time = race_time_match.group(1) if race_time_match else None

            if not race_time:
                continue

            course_header = caption.find_parent('div', class_='panel').find('h2')
            course_name = course_header.get_text(strip=True) if course_header else None

            if not course_name:
                continue

            race_table = caption.find_next_sibling('table')
            if not race_table:
                continue

            horses = []
            for row in race_table.find('tbody').find_all('tr'):
                cells = row.find_all('td')
                if not cells:
                    continue

                horse_name = cells[0].get_text(strip=True)
                current_odds_str = cells[1].get_text(strip=True)

                horses.append({
                    'name': horse_name,
                    'odds_str': current_odds_str,
                    'odds_float': convert_odds_to_float(current_odds_str)
                })

            if not horses:
                continue

            horses.sort(key=lambda x: x['odds_float'])

            key = (normalize_track_name(course_name), race_time)
            atr_races_lookup[key] = {
                'course': course_name,
                'time': race_time,
                'field_size': len(horses),
                'favorite': horses[0] if len(horses) > 0 else None,
                'second_favorite': horses[1] if len(horses) > 1 else None,
            }

    print(f"✅ AtTheRaces scan complete. Found data for {len(atr_races_lookup)} races.")
    return atr_races_lookup

def generate_mode_A_report(races: list[dict]):
    """Generates the 'Perfect Tipsheet' HTML report."""
    title = "Perfect Tipsheet"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"Perfect_Tipsheet_{timestamp}.html"

    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f4f8; color: #333; margin: 20px; }
        .container { max-width: 800px; margin: auto; background: #fff; padding: 25px; border-radius: 10px; box-shadow: 0 5px 15px rgba(0,0,0,0.1); }
        h1 { color: #1a73e8; text-align: center; border-bottom: 3px solid #1a73e8; padding-bottom: 10px; }
        .race-card { border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-left: 5px solid #1a73e8; background-color: #fff; border-radius: 8px; }
        .race-header { font-size: 1.5em; font-weight: bold; color: #333; margin-bottom: 15px; }
        .race-meta { font-size: 1.1em; color: #5f6368; margin-bottom: 15px; }
        .horse-details { margin-top: 10px; padding: 10px; border-radius: 5px; background-color: #f8f9fa; }
        .horse-details b { color: #1a73e8; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""

    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""

    if not races:
        html_body += "<p>No races met the specified criteria of Field Size < 7, Favorite >= 1/1, and 2nd Favorite >= 3/1.</p>"
    else:
        html_body += f"<p>Found {len(races)} races that meet all analytical criteria.</p>"
        for race in races:
            fav = race['favorite']
            sec_fav = race['second_favorite']
            html_body += f"""
            <div class="race-card">
                <div class="race-header">{race['course']} - Race Time: {race['time']}</div>
                <div class="race-meta">Field Size: {race['field_size']} Runners</div>
                <div class="horse-details"><b>Favorite:</b> {fav['name']} (<b>{fav['odds_str']}</b>)</div>
                <div class="horse-details"><b>2nd Favorite:</b> {sec_fav['name']} (<b>{sec_fav['odds_str']}</b>)</div>
            </div>
            """

    report_time = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
    html_end = f'<div class="footer"><p>Report generated on {report_time}</p></div></div></body></html>'
    final_html = html_start + html_body + html_end

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(final_html)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e:
        print(f"\n❌ Error saving the report: {e}")

def run_mode_A(master_race_list: list[dict]):
    """
    Executes the full workflow for unrestricted mode.
    """
    print("\n-- Running Mode A: Unrestricted Workflow --")

    # 1. Filter master list for small fields
    small_field_races = [r for r in master_race_list if r['field_size'] < 7]
    print(f"Found {len(small_field_races)} races with fewer than 7 runners.")

    if not small_field_races:
        print("No small-field races to analyze.")
        # Generate an empty report to signify completion
        generate_mode_A_report([])
        return

    # 2. Get live odds data from ATR
    atr_regions = ['uk', 'ireland', 'usa', 'france', 'saf', 'aus']
    atr_odds_data = fetch_atr_odds_data(atr_regions)

    if not atr_odds_data:
        print("Could not fetch any live odds data from AtTheRaces. Cannot perform analysis.")
        return

    # 3. Correlate and apply final filter
    print("\n🔍 Analyzing races against the final criteria (in next 30 mins, Fav >= 1/1, 2nd Fav >= 3/1)...")
    now_utc = datetime.now(pytz.utc)
    thirty_mins_from_now = now_utc + timedelta(minutes=30)

    perfect_tips = []
    for race in small_field_races:
        # Time-based filter: race must be upcoming and start in the next 30 mins
        if not (race['datetime_utc'] and now_utc < race['datetime_utc'] < thirty_mins_from_now):
            continue

        # Use normalized course name for matching
        key = (normalize_track_name(race['course']), race['time'])

        if key in atr_odds_data:
            atr_data = atr_odds_data[key]
            fav = atr_data.get('favorite')
            sec_fav = atr_data.get('second_favorite')

            if not fav or not sec_fav:
                continue

            # The ultimate filter criteria for odds
            fav_odds_ok = fav['odds_float'] >= 1.0
            sec_fav_odds_ok = sec_fav['odds_float'] >= 3.0

            if fav_odds_ok and sec_fav_odds_ok:
                # We have a match! Add odds info to the race dict
                race['favorite'] = fav
                race['second_favorite'] = sec_fav
                perfect_tips.append(race)
                print(f"   ✅ MATCH: {race['course']} {race['time']}")

    # Sort the final list chronologically before generating the report
    perfect_tips.sort(key=lambda r: r['datetime_utc'])

    # 4. Generate and save the report
    generate_mode_A_report(perfect_tips)

# ==============================================================================
# MODE B: RESTRICTED WORKFLOW
# ==============================================================================

class RacingAndSportsFetcher:
    """
    Fetches and processes meeting data from the Racing & Sports JSON endpoint.
    Adapted from RacingAndSports_RacingMonitor_continuous.py.
    """
    def __init__(self, api_url):
        self.api_url = api_url
        self.session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.racingandsports.com.au/todays-racing',
        }
        self.session.headers.update(headers)

    def fetch_data(self):
        """Fetches the main JSON directory of all meetings."""
        print("-> Fetching R&S main meeting directory...")
        try:
            response = self.session.get(self.api_url, timeout=30, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"   ❌ ERROR: Could not fetch R&S JSON directory: {e}")
        except json.JSONDecodeError:
            print("   ❌ ERROR: Failed to decode R&S main directory. Not valid JSON.")
        return None

    def process_meetings_data(self, json_data):
        """Processes the main JSON data to get a list of all meetings."""
        if not isinstance(json_data, list):
            return None

        processed_meetings = []
        for discipline_data in json_data:
            for country_data in discipline_data.get("Countries", []):
                for meeting_data in country_data.get("Meetings", []):
                    course_name = meeting_data.get("Course")
                    final_link = meeting_data.get("PDFUrl") or meeting_data.get("PreMeetingUrl")

                    if course_name and final_link:
                        processed_meetings.append({
                            'course': course_name,
                            'link': final_link,
                        })
        return processed_meetings

def build_rs_lookup_table(rs_meetings):
    """
    Processes R&S data into a lookup table.
    Key: (normalized_track_name, date_YYYY-MM-DD), Value: R&S meeting link.
    """
    lookup = {}
    if not rs_meetings: return lookup

    print("...Building Racing & Sports lookup table for matching...")
    for meeting in rs_meetings:
        link = meeting.get('link')
        match = re.search(r'/(\d{4}-\d{2}-\d{2})', link)
        if not match: continue

        date_str = match.group(1)
        normalized_course = normalize_track_name(meeting['course'])
        lookup[(normalized_course, date_str)] = link

    print(f"   ✅ Lookup table created with {len(lookup)} R&S entries.")
    return lookup

def find_rs_link(sky_track: str, sky_date_iso: str, lookup_table: dict):
    """
    Finds a matching R&S meeting link using the lookup table with flexibility.
    """
    normalized_sky_track = normalize_track_name(sky_track)

    # First, try a direct match on the key.
    direct_key = (normalized_sky_track, sky_date_iso)
    if direct_key in lookup_table:
        return lookup_table[direct_key]

    # If that fails, iterate for a flexible substring match on the same date.
    for (rs_track_normalized, rs_date), rs_link in lookup_table.items():
        if rs_date == sky_date_iso:
            if normalized_sky_track in rs_track_normalized or rs_track_normalized in normalized_sky_track:
                return rs_link

    return None

def generate_mode_B_report(races: list[dict]):
    """Generates the 'Actionable Link List' HTML report."""
    title = "Upcoming Small-Field Races"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"Actionable_Link_List_{timestamp}.html"

    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f4f4f9; color: #333; margin: 20px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        h1 { color: #5a2d82; text-align: center; border-bottom: 3px solid #5a2d82; padding-bottom: 10px; }
        .course-group { margin-bottom: 30px; }
        .course-header { font-size: 1.6em; font-weight: bold; color: #333; padding-bottom: 10px; border-bottom: 2px solid #eee; margin-bottom: 15px; }
        .race-entry { border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fafafa; }
        .race-details { font-weight: bold; font-size: 1.1em; color: #333; margin-bottom: 10px; }
        .race-links a, .race-links span { display: inline-block; text-decoration: none; padding: 8px 15px; border-radius: 4px; margin: 5px 10px 5px 0; font-weight: bold; min-width: 160px; text-align: center; }
        a.sky-link { background-color: #007bff; color: white; } a.sky-link:hover { background-color: #0056b3; }
        a.rs-link { background-color: #dc3545; color: white; } a.rs-link:hover { background-color: #c82333; }
        span.rs-ignored-tag { color: #6c757d; background-color: #fff; border: 1px solid #ccc; cursor: default; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""

    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""

    if not races:
        html_body += "<p>No races with fewer than 7 runners were found on Sky Sports today.</p>"
    else:
        races_by_course = {}
        for race in races:
            races_by_course.setdefault(race['course'], []).append(race)

        # Preserve original race order by iterating through the original list
        # to determine the order of courses.
        course_order = []
        seen_courses = set()
        for race in races:
            if race['course'] not in seen_courses:
                course_order.append(race['course'])
                seen_courses.add(race['course'])

        for course in course_order:
            course_races = races_by_course[course]
            html_body += f'<div class="course-group"><div class="course-header">{course}</div>'
            for race in course_races: # Races are already sorted
                html_body += f'<div class="race-entry"><p class="race-details">Race at {race["time"]} ({race["field_size"]} runners)</p><div class="race-links">'
                html_body += f'<a href="{race["race_url"]}" target="_blank" class="sky-link">Sky Sports Racecard</a>'

                if race.get("rs_link"):
                    html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Meeting Form</a>'
                else:
                    # Only show the "Ignored" tag for UK/Ireland races
                    if race['country'].lower() in ['uk', 'ireland', 'united kingdom', 'ire']:
                         html_body += '<span class="rs-ignored-tag">Ignored by R&S</span>'

                html_body += '</div></div>'
            html_body += '</div>'

    report_time = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
    html_end = f'<div class="footer"><p>Report generated on {report_time}</p></div></div></body></html>'
    final_html = html_start + html_body + html_end

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(final_html)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e:
        print(f"\n❌ Error saving the report: {e}")

def run_mode_B(master_race_list: list[dict]):
    """Executes the full workflow for restricted mode."""
    print("\n-- Running Mode B: Restricted Workflow --")

    # 1. Filter master list for small fields
    small_field_races = [r for r in master_race_list if r['field_size'] < 7]
    print(f"Found {len(small_field_races)} races with fewer than 7 runners.")

    if not small_field_races:
        print("No small-field races to generate a report for.")
        generate_mode_B_report([])
        return

    # 2. Get R&S meeting data to build a lookup table
    print("\n🗞️ Fetching data from Racing & Sports...")
    rs_api_url = "https://www.racingandsports.com.au/todays-racing-json-v2"
    link_fetcher = RacingAndSportsFetcher(rs_api_url)
    json_data = link_fetcher.fetch_data()
    all_rs_meetings = link_fetcher.process_meetings_data(json_data) if json_data else []
    rs_lookup_table = build_rs_lookup_table(all_rs_meetings)

    # 3. Enrich the race list with R&S links
    print("\n🔗 Matching Sky Sports races with R&S links (for UK & Ireland)...")
    enriched_races = []
    for race in small_field_races:
        race['rs_link'] = None  # Default to no link
        if race['country'].upper() in ['UK', 'IRE']:
            date_str_iso = race.get('date_iso')
            if not date_str_iso:
                print(f"   -> {race['course']} @ {race['time']}: Could not find date_iso in race data.")
                enriched_races.append(race)
                continue

            rs_link = find_rs_link(race['course'], date_str_iso, rs_lookup_table)
            if rs_link:
                race['rs_link'] = rs_link
                print(f"   -> {race['course']} @ {race['time']}: R&S Link FOUND")
            else:
                print(f"   -> {race['course']} @ {race['time']}: No R&S link found")

        enriched_races.append(race)

    # 4. Sort, filter, and limit the list
    enriched_races = sort_and_limit_races(enriched_races)

    # 5. Generate and save the report
    generate_mode_B_report(enriched_races)

# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================

def main():
    """The main orchestration function."""
    print("=" * 80)
    print("🚀 Unified Racing Report Generator")
    print("=" * 80)

    # Step 1: Universal Scan
    SKYSPORTS_URL = "https://www.skysports.com/racing/racecards"
    sky_sports_html = fetch_page(SKYSPORTS_URL)
    master_race_list = universal_sky_sports_scan(sky_sports_html, SKYSPORTS_URL)

    if not master_race_list:
        print("\nCould not retrieve the master race list from Sky Sports. Exiting.")
        return

    # Step 2: Environmental Check
    is_unrestricted = check_attheraces_connectivity()

    if is_unrestricted:
        run_mode_A(master_race_list)
    else:
        run_mode_B(master_race_list)


if __name__ == "__main__":
    main()
