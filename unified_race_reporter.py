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
    Attempts to fetch a URL using three methods: a standard request, a
    Chrome browser-impersonating request, and finally an iPhone-impersonating
    request.
    """
    # Attempt 1: Standard "robot" request
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20, verify=False)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        print(f"   -> Standard request to {url} failed. Trying Chrome browser impersonation...")

    # Attempt 2: "Browser" impersonation with Chrome
    try:
        session = CurlCffiSession(impersonate="chrome110")
        response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception:
        print(f"   -> Chrome impersonation failed. Trying iPhone impersonation...")

    # Attempt 3: "Browser" impersonation with iPhone User-Agent
    try:
        iphone_ua = 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1'
        response = requests.get(url, headers={'User-Agent': iphone_ua}, timeout=20, verify=False)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
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

def convert_odds_to_float(odds_str: str) -> float:
    """Converts fractional or decimal odds string to a float for sorting."""
    if not isinstance(odds_str, str) or not odds_str.strip():
        return 999.0

    odds_str = odds_str.strip().upper().replace('-', '/')
    if odds_str == 'SP':
        return 999.0
    if odds_str == 'EVS':
        return 1.0

    if '/' in odds_str:
        try:
            num, den = map(float, odds_str.split('/'))
            if den == 0: return 999.0
            return num / den
        except (ValueError, ZeroDivisionError):
            return 999.0

    # Handle decimal odds as a fallback
    try:
        decimal_odds = float(odds_str)
        if decimal_odds > 0:
            return decimal_odds - 1.0
    except ValueError:
        pass

    return 999.0

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
    master_race_list = []

    for region in regions:
        # For Australia, check both today and tomorrow to account for timezone differences.
        dates_to_check = [datetime.now()]
        if region == 'aus':
            dates_to_check.append(datetime.now() + timedelta(days=1))

        for date_to_scan in dates_to_check:
            today_str_url = date_to_scan.strftime('%Y-%m-%d')
            today_str_atr = date_to_scan.strftime('%Y%m%d')

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
                    'data_source': 'AtTheRaces'
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
        .race-entry { border: 1px solid #dee2e6; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fff; }
        .race-entry.analytical-match { border-left: 5px solid #28a745; }
        .race-details { font-weight: bold; font-size: 1.1em; color: #343a40; margin-bottom: 10px; }
        .race-details .course-name { font-size: 1.2em; color: #495057; }
        .race-links a, .race-links span { display: inline-block; text-decoration: none; padding: 8px 15px; border-radius: 4px; margin: 5px 10px 5px 0; font-weight: bold; min-width: 160px; text-align: center; }
        a.racecard-link { background-color: #17a2b8; color: white; }
        a.racecard-link:hover { background-color: #138496; }
        a.racecard-link.atr { background-color: #007bff; }
        a.racecard-link.atr:hover { background-color: #0056b3; }
        a.racecard-link.oddschecker { background-color: #ff8c00; }
        a.racecard-link.oddschecker:hover { background-color: #cc7000; }
        a.racecard-link.sky { background-color: #6c757d; }
        a.racecard-link.sky:hover { background-color: #5a6268; }
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
        # Sort all races chronologically, removing course grouping
        sorted_races = sorted(races, key=lambda r: r.get('datetime_utc', r.get('time', '')))

        for race in sorted_races:
            display_time = convert_utc_to_eastern(race.get('datetime_utc')) or f"{race.get('time', 'N/A').replace(':', 'h')} (Timezone Unknown)"
            course_details = f"{race['course']} ({race['country']})"

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
            html_body += f'<p class="race-details"><span class="course-name">{course_details}</span> - Race at {display_time} ({race["field_size"]} runners)</p>'

            # --- Always show links ---
            html_body += '<div class="race-links">'
            source = race.get('data_source', 'Unknown')
            link_text = "Racecard"
            link_class = "racecard-link"
            if source == 'AtTheRaces':
                link_text = "ATR Racecard"
                link_class += " atr"
            elif source == 'Oddschecker':
                link_text = "Oddschecker Card"
                link_class += " oddschecker"
            elif source == 'SkySports':
                link_text = "Sky Racecard"
                link_class += " sky"
            elif source == 'rpb2b':
                link_text = "Racecard (Sky)"
                link_class += " sky"

            html_body += f'<a href="{race["race_url"]}" target="_blank" class="{link_class}">{link_text}</a>'
            if race.get("rs_link"):
                html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Form</a>'
            else:
                html_body += '<span class="rs-ignored-tag">No R&S Form</span>'
            html_body += '</div>'

            # --- Show odds info if available ---
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
# NEW DATA SOURCE: DRF/FANDUEL API
# ==============================================================================
def fetch_races_from_drf_api():
    """
    Fetches race data from the DRF/FanDuel JSON API. This is a primary source
    for North American race data with odds.
    """
    print("\nℹ️ Fetching data from DRF/FanDuel API...")

    master_race_list = []
    # Check today and tomorrow to get a complete picture
    dates_to_check = [datetime.now(), datetime.now() + timedelta(days=1)]

    for date_to_scan in dates_to_check:
        date_str = date_to_scan.strftime('%Y-%m-%d')
        api_url = f"https://drf-api.akamaized.net/api/card/list?cardDate={date_str}"
        print(f"-> Querying DRF API for {date_str}: {api_url}")

        try:
            api_text = robust_fetch(api_url)
            api_data = json.loads(api_text)
        except (ConnectionError, json.JSONDecodeError) as e:
            print(f"   ❌ Failed to fetch or parse from DRF API for {date_str}: {e}")
            continue

        if not isinstance(api_data, list):
            print(f"   ⚠️ DRF API response for {date_str} is not a list. Skipping.")
            continue

        for meeting in api_data:
            course_name = meeting.get('trackName')
            country_code = meeting.get('country')
            races = meeting.get('races')

            if not all([course_name, country_code, races]) or not isinstance(races, list):
                continue

            for race in races:
                race_num = race.get('raceNumber')
                post_time_str = race.get('postTime') # Full datetime string
                entries = race.get('entries')

                if not all([race_num, post_time_str, entries]) or not isinstance(entries, list):
                    continue

                track_code = meeting.get('trackId', '').upper()
                race_url = f"https://racing.fanduel.com/race-card/{track_code}/{date_str}/R{race_num}"

                horses = []
                for entry in entries:
                    horse_name = entry.get('horseName')
                    # Odds can be in a few places, try to find them
                    odds_str = entry.get('morningLineOdds') or entry.get('currentOdds')

                    if horse_name and odds_str:
                         horses.append({
                            'name': horse_name,
                            'odds_str': str(odds_str),
                            'odds_float': convert_odds_to_float(str(odds_str))
                        })

                if not horses:
                    continue

                horses.sort(key=lambda x: x['odds_float'])

                # Extract just the time for the 'time' field
                try:
                    time_only = datetime.fromisoformat(post_time_str.replace('Z', '+00:00')).strftime('%H:%M')
                except ValueError:
                    time_only = "N/A"

                master_race_list.append({
                    'course': course_name,
                    'time': time_only,
                    'datetime_utc': post_time_str,
                    'field_size': len(horses),
                    'country': country_code,
                    'race_url': race_url,
                    'favorite': horses[0] if len(horses) > 0 else None,
                    'second_favorite': horses[1] if len(horses) > 1 else None,
                    'data_source': 'DRF/FanDuel'
                })

    print(f"✅ DRF/FanDuel API scan complete. Found {len(master_race_list)} races.")
    return master_race_list

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
                'data_source': 'Oddschecker'
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
                'data_source': 'rpb2b'
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
        if not master_race_list: raise ConnectionError("ATR returned no data.")
        print("\n✅ Primary source successful. Using rich data from AtTheRaces.")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to fallback source #1.")

    # --- Attempt 2: DRF/FanDuel API ---
    try:
        print("\n--- Attempting Fallback Source #1: DRF/FanDuel API ---")
        master_race_list = fetch_races_from_drf_api()
        if not master_race_list: raise ConnectionError("DRF API returned no data.")
        print("\n✅ DRF/FanDuel API successful. Using NA data with odds.")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to fallback source #2.")

    # --- Attempt 3: Fallback Scraper (Oddschecker) ---
    try:
        print("\n--- Attempting Fallback Source #2: Oddschecker Scraper ---")
        master_race_list = scrape_oddschecker()
        if not master_race_list: raise ConnectionError("Oddschecker returned no data.")
        print("\n✅ Oddschecker scrape successful. Using global data (no live odds).")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to fallback source #3.")

    # --- Attempt 4: Fallback API (rpb2b.com) ---
    try:
        print("\n--- Attempting Fallback Source #3: rpb2b.com API ---")
        master_race_list = fetch_races_from_rpb2b_api()
        if not master_race_list: raise ConnectionError("rpb2b.com API returned no data.")
        print("\n✅ rpb2b.com API successful. Using NA-only data.")
        return master_race_list
    except ConnectionError as e:
        print(f"\n⚠️ {e} Moving to final fallback source.")

    # --- Attempt 5: Final Fallback Scraper (Sky Sports) ---
    try:
        print("\n--- Attempting Final Fallback Source #4: Sky Sports Scraper ---")
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
                'data_source': 'SkySports'
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
        print(f"\n🔍 Applying filters to {len(master_race_list)} races...")
        now_utc = datetime.now(ZoneInfo("UTC"))
        now_naive = datetime.now()

        filtered_races = []
        for race in master_race_list:
            # Filter 1: Field size must be 7 or less
            if race.get('field_size', 99) > 7:
                continue

            # Filter 2: Race must be in the future
            race_dt_str = race.get('datetime_utc')
            if not race_dt_str:
                continue

            is_future_race = False
            try:
                if 'Z' in race_dt_str or '+' in race_dt_str:
                    race_dt = datetime.fromisoformat(race_dt_str.replace('Z', '+00:00'))
                    if race_dt > now_utc:
                        is_future_race = True
                else:
                    race_dt = datetime.strptime(race_dt_str, '%Y-%m-%d %H:%M')
                    if race_dt > now_naive:
                        is_future_race = True
            except ValueError:
                continue # Skip races with unparsable time

            if is_future_race:
                filtered_races.append(race)

        print(f"   ✅ Found {len(filtered_races)} races matching all criteria.")

        if filtered_races:
            generate_unified_report(filtered_races)
        else:
            print("\n❌ No future races matching the criteria were found.")
    else:
        print("\n❌ Could not retrieve data from any source. Exiting.")

    print("\n🏁 Script finished.")


if __name__ == "__main__":
    main()
