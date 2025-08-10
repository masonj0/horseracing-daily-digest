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

# --- Third-Party Libraries ---
import requests
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
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
            response.raise_for_status()
            html_content = response.text
        except requests.exceptions.RequestException as e:
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

            master_race_list.append({
                'course': course_name,
                'time': race_time,
                'field_size': len(horses),
                'country': region.upper(),
                'race_url': race_url,
                'favorite': horses[0] if len(horses) > 0 else None,
                'second_favorite': horses[1] if len(horses) > 1 else None,
            })

    print(f"✅ ATR Universal Scan complete. Found {len(master_race_list)} races globally.")
    return master_race_list


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
                <div class="race-header">{race['course']} ({race['country']}) - Race Time: {race['time']}</div>
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
    Filters the rich master list from ATR to find races meeting the criteria
    and generates the 'Perfect Tipsheet' report.
    """
    print("🔍 Filtering global race list for analytical matches...")

    perfect_tips = []
    for race in master_race_list:
        field_size_ok = race['field_size'] < 7
        fav = race.get('favorite')
        sec_fav = race.get('second_favorite')

        if not (fav and sec_fav):
            continue

        fav_odds_ok = fav['odds_float'] >= 1.0
        sec_fav_odds_ok = sec_fav['odds_float'] >= 3.0

        if field_size_ok and fav_odds_ok and sec_fav_odds_ok:
            perfect_tips.append(race)
            print(f"   ✅ MATCH: {race['course']} ({race['country']}) at {race['time']}")

    generate_mode_A_report(perfect_tips)

# ==============================================================================
# MODE B: RESTRICTED WORKFLOW
# ==============================================================================

class RacingAndSportsFetcher:
    """
    Fetches and processes meeting data from the Racing & Sports JSON endpoint.
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
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"   ❌ ERROR: Could not fetch R&S JSON directory: {e}")
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

def find_rs_link(race_course: str, race_url: str, lookup_table: dict):
    """
    Finds a matching R&S meeting link using the lookup table with flexibility.
    """
    try:
        date_str_iso = re.search(r'/(\d{4}-\d{2}-\d{2})', race_url).group(1)
    except AttributeError:
        return None # Cannot find a date in the URL to match on.

    normalized_sky_track = normalize_track_name(race_course)

    direct_key = (normalized_sky_track, date_str_iso)
    if direct_key in lookup_table:
        return lookup_table[direct_key]

    for (rs_track_normalized, rs_date), rs_link in lookup_table.items():
        if rs_date == date_str_iso:
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
        a.atr-link { background-color: #007bff; color: white; } a.atr-link:hover { background-color: #0056b3; }
        a.rs-link { background-color: #dc3545; color: white; } a.rs-link:hover { background-color: #c82333; }
        span.rs-ignored-tag { color: #6c757d; background-color: #fff; border: 1px solid #ccc; cursor: default; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""

    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""

    if not races:
        html_body += "<p>No races with fewer than 7 runners were found from the fallback data source.</p>"
    else:
        races_by_course = {}
        for race in races:
            races_by_course.setdefault(f"{race['course']} ({race['country']})", []).append(race)

        for course, course_races in sorted(races_by_course.items()):
            html_body += f'<div class="course-group"><div class="course-header">{course}</div>'
            for race in sorted(course_races, key=lambda r: r['time']):
                html_body += f'<div class="race-entry"><p class="race-details">Race at {race["time"]} ({race["field_size"]} runners)</p><div class="race-links">'
                html_body += f'<a href="{race["race_url"]}" target="_blank" class="atr-link">ATR Racecard</a>'

                if race.get("rs_link"):
                    html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Meeting Form</a>'
                else:
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
    """
    Executes the full workflow for restricted mode using fallback data.
    """
    small_field_races = [r for r in master_race_list if r['field_size'] < 7]
    print(f"Found {len(small_field_races)} races with fewer than 7 runners from fallback source.")

    if not small_field_races:
        print("No small-field races to generate a report for.")
        generate_mode_B_report([])
        return

    print("\n🗞️ Fetching data from Racing & Sports to find matching form guides...")
    rs_api_url = "https://www.racingandsports.com.au/todays-racing-json-v2"
    link_fetcher = RacingAndSportsFetcher(rs_api_url)
    json_data = link_fetcher.fetch_data()
    all_rs_meetings = link_fetcher.process_meetings_data(json_data) if json_data else []
    rs_lookup_table = build_rs_lookup_table(all_rs_meetings)

    print("\n🔗 Attempting to match all races with R&S links...")
    enriched_races = []
    for race in small_field_races:
        rs_link = find_rs_link(race['course'], race['race_url'], rs_lookup_table)
        if rs_link:
            race['rs_link'] = rs_link
            print(f"   -> {race['course']} @ {race['time']}: R&S Link FOUND")

        enriched_races.append(race)

    generate_mode_B_report(enriched_races)

# ==============================================================================
# FALLBACK DATA SOURCE (for Restricted Mode)
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
        response = requests.get(api_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        api_data = response.json()
        print("   ✅ Success.")
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
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
                'field_size': num_runners,
                'race_url': sky_meeting_url,
                'country': country_code,
                'favorite': None,
                'second_favorite': None,
            })
    return fallback_race_list

# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================

def main():
    """The main orchestration function."""
    print("=" * 80)
    print("🚀 Unified Racing Report Generator")
    print("=" * 80)

    try:
        # Step 1: Attempt the primary universal scan from AtTheRaces
        atr_regions = ['uk', 'ireland', 'usa', 'france', 'saf', 'aus']
        master_race_list = universal_atr_scan(atr_regions)

        # If the scan succeeds, we are in UNRESTRICTED MODE
        print("\n-- Running Mode A: Unrestricted Workflow --")
        if not master_race_list:
            print("\nNo races found from AtTheRaces today. Exiting.")
            return
        run_mode_A(master_race_list)

    except ConnectionError:
        # ATR scan failed, so we are in RESTRICTED MODE
        print("\n⚠️ Primary ATR source failed. Switching to Restricted Mode.")

        # Use the rpb2b.com API as a fallback data source
        fallback_race_list = fetch_races_from_rpb2b_api()

        if not fallback_race_list:
            print("\n❌ All data sources failed. Cannot generate a report. Exiting.")
            return

        # Run Mode B logic with the fallback data
        print("\n-- Running Mode B: Restricted Workflow (with fallback data) --")
        run_mode_B(fallback_race_list)


if __name__ == "__main__":
    main()
