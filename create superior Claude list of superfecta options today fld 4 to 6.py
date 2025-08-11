import requests
import subprocess
import shutil
import platform
import certifi
import trafilatura
import os
import sys
import time
import msvcrt
import re
import webbrowser
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
from curl_cffi.requests import Session as CurlCffiSession
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

# Force UTF-8 encoding on Windows
if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    
# Suppress only the single InsecureRequestWarning from urllib3 needed for verify=False
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Define data sources (from the advanced scanner)
@dataclass
class RaceInfo:
    source: str
    track: str
    date: str
    time: str
    details: str
    runner_count: int
    racecard_url: str
    result_url: Optional[str] = None
    discipline: str = "thoroughbred"  # thoroughbred, greyhound, harness
    country: str = "GB"

class DataSource:
    def __init__(self, name: str, base_url: str, discipline: str, country: str):
        self.name = name
        self.base_url = base_url
        self.discipline = discipline
        self.country = country
    
    def get_urls_for_date(self, date: datetime) -> List[str]:
        """Override in subclasses"""
        return []

class SkySourceBase(DataSource):
    """Base class for Sky Sports sources"""
    
    def get_urls_for_date(self, date: datetime) -> List[str]:
        return [self.base_url]

class AtTheRacesSource(DataSource):
    """At The Races - Multiple regions"""
    
    def __init__(self):
        super().__init__("AtTheRaces", "https://www.attheraces.com", "thoroughbred", "Multi")
        self.regions = ["uk", "ireland", "usa", "france", "saf", "aus"]
    
    def get_urls_for_date(self, date: datetime) -> List[str]:
        date_str = date.strftime('%Y%m%d')
        urls = []
        for region in self.regions:
            urls.append(f"{self.base_url}/ajax/marketmovers/tabs/{region}/{date_str}")
        return urls

class SportingLifeGreyhoundsSource(DataSource):
    """Sporting Life Greyhounds"""
    
    def __init__(self):
        super().__init__("SportingLife", "https://www.sportinglife.com/greyhounds/racecards", "greyhound", "GB")

class HarnessRacingAustraliaSource(DataSource):
    """Harness Racing Australia"""
    
    def __init__(self):
        super().__init__("HarnessAustralia", "https://www.harness.org.au", "harness", "AU")
    
    def get_urls_for_date(self, date: datetime) -> List[str]:
        date_str = date.strftime('%d/%m/%Y')
        return [f"{self.base_url}/racing/fields/?firstDate={date_str}&submit=DISPLAY"]

class StandardbredCanadaSource(DataSource):
    """Standardbred Canada"""
    
    def __init__(self):
        super().__init__("StandardbredCanada", "https://standardbredcanada.ca", "harness", "CA")
    
    def get_urls_for_date(self, date: datetime) -> List[str]:
        date_str = date.strftime('%Y-%m-%d')
        return [f"{self.base_url}/racing/entries/date/{date_str}"]

# Initialize all data sources
DATA_SOURCES = [
    SkySourceBase("SkyRacing", "https://www.skysports.com/racing/racecards", "thoroughbred", "GB"),
    AtTheRacesSource(),
    SportingLifeGreyhoundsSource(),
    HarnessRacingAustraliaSource(),
    StandardbredCanadaSource()
]

# Your existing robust fetching methods
def try_curl_cffi(url):
    """Method 1: Browser Impersonation (curl_cffi)"""
    print(f"  🔄 Trying curl_cffi for {urlparse(url).netloc}")
    try:
        session = CurlCffiSession(impersonate="chrome120", timeout=20)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = session.get(url, headers=headers, verify=certifi.where())
        response.raise_for_status()
        print(f"    ✅ Success with curl_cffi!")
        return response.text
    except Exception as e:
        print(f"    ❌ curl_cffi failed: {str(e)[:100]}")
        return None

def try_requests_with_variations(url):
    """Method 2: Requests with URL variations"""
    print(f"  🔄 Trying requests variations for {urlparse(url).netloc}")
    
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace('m.', '').replace('www.', '')
    
    url_variations = {
        url,
        f"https://www.{domain}{parsed_url.path}",
        f"https://{domain}{parsed_url.path}",
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }

    for i, test_url in enumerate(url_variations):
        try:
            response = requests.get(test_url, headers=headers, verify=False, timeout=15, allow_redirects=True)
            if response.status_code == 200 and len(response.text) > 500:
                print(f"    ✅ Success with requests!")
                return response.text
        except requests.exceptions.RequestException:
            continue
    
    print(f"    ❌ All requests variations failed")
    return None

def try_subprocess_curl(url):
    """Method 3: System curl command"""
    print(f"  🔄 Trying subprocess curl for {urlparse(url).netloc}")
    
    if not shutil.which("curl"):
        print(f"    ❌ 'curl' command not found")
        return None
        
    try:
        curl_cmd = [
            'curl', '--silent', '--show-error', '--location', '--compressed',
            '--max-time', '30', '--connect-timeout', '10',
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            '--header', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            '--ssl-no-revoke', url
        ]
        result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=45, check=False)
        
        if result.returncode == 0 and result.stdout:
            print(f"    ✅ Success with subprocess curl!")
            return result.stdout
        else:
            print(f"    ❌ Subprocess curl failed")
            return None
            
    except Exception:
        print(f"    ❌ Subprocess curl exception")
        return None

def fetch_with_all_methods(url):
    """Try all methods to fetch a URL"""
    for method in [try_curl_cffi, try_requests_with_variations, try_subprocess_curl]:
        result = method(url)
        if result:
            return result
        time.sleep(0.5)  # Brief pause between methods
    return None

def extract_runner_count(details_text):
    """Extract runner count from text"""
    match = re.search(r'(\d+)\s+runners?', details_text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 999

def parse_sky_sports_races(html_content, source_url):
    """Parse Sky Sports racing page (your original logic)"""
    races = []
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_url = urlparse(source_url)
    
    event_containers = soup.find_all('div', class_='sdc-site-racing-meetings__event')
    
    for container in event_containers:
        result_url, racecard_url, details_text = None, None, ""

        # Extract URLs
        result_tag = container.find('a', class_='sdc-site-racing-meetings__event-result')
        if result_tag and result_tag.get('href'):
            result_url = f"{parsed_url.scheme}://{parsed_url.netloc}{result_tag.get('href')}"

        racecard_tag = container.find('a', class_='sdc-site-racing-meetings__event-link')
        if racecard_tag and racecard_tag.get('href'):
            racecard_url = f"{parsed_url.scheme}://{parsed_url.netloc}{racecard_tag.get('href')}"

        # Extract Details Text
        race_name_span = container.find('span', class_='sdc-site-racing-meetings__event-name')
        race_details_span = container.find('span', class_='sdc-site-racing-meetings__event-details')
        details_parts = []
        if race_name_span: details_parts.append(race_name_span.get_text(strip=True))
        if race_details_span: details_parts.append(race_details_span.get_text(strip=True))
        details_text = ' '.join(details_parts)

        if details_text and racecard_url:
            # Parse URL for track and date
            track, race_date = parse_race_url_for_info(racecard_url)
            race_time = extract_time_from_details(details_text)
            
            full_details = f"[{track.upper()}] {race_date} {race_time} - {details_text}" if track and race_date else details_text
            runner_count = extract_runner_count(details_text)
            
            race = RaceInfo(
                source="SkyRacing",
                track=track or "Unknown",
                date=race_date or "Unknown",
                time=race_time or "Unknown",
                details=full_details,
                runner_count=runner_count,
                racecard_url=racecard_url,
                result_url=result_url,
                discipline="thoroughbred",
                country="GB"
            )
            races.append(race)
    
    return races

def parse_atr_races(html_content, source_url, region):
    """Parse At The Races market movers data"""
    races = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for race time captions
        for caption in soup.find_all("caption", string=re.compile(r"^\d{2}:\d{2}")):
            race_time_match = re.match(r"(\d{2}:\d{2})", caption.get_text(strip=True))
            if not race_time_match:
                continue
            race_time = race_time_match.group(1)

            # Find course name
            panel = caption.find_parent("div", class_=re.compile(r"\bpanel\b")) or caption.find_parent("div")
            course_header = None
            if panel:
                course_header = panel.find("h2")
            if not course_header:
                course_header = caption.find_previous("h2")
            if not course_header:
                continue
            course_name = course_header.get_text(strip=True)

            # Count runners in table
            table = caption.find_next_sibling("table") or caption.find_parent().find("table")
            runner_count = 0
            if table:
                body = table.find("tbody") or table
                for row in body.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2 and cells[0].get_text(strip=True):
                        runner_count += 1

            if runner_count > 0:
                # Build race URL
                course_slug = re.sub(r"\s+", "-", course_name.strip().lower())
                today_str = datetime.now().strftime("%Y-%m-%d")
                time_slug = race_time.replace(":", "")
                race_url = f"https://www.attheraces.com/racecard/{course_slug}/{today_str}/{time_slug}"
                
                details = f"[{course_name.upper()}] {today_str} {race_time} - {region.upper()} ({runner_count} runners)"
                
                race = RaceInfo(
                    source="AtTheRaces",
                    track=course_name,
                    date=today_str,
                    time=race_time,
                    details=details,
                    runner_count=runner_count,
                    racecard_url=race_url,
                    result_url=None,
                    discipline="thoroughbred",
                    country=region.upper()
                )
                races.append(race)
                
    except Exception as e:
        print(f"    ⚠️ Error parsing ATR data: {str(e)[:100]}")
    
    return races

def parse_sporting_life_greyhounds(html_content, source_url):
    """Parse Sporting Life greyhounds"""
    races = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for meeting links
        meeting_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.match(r"^/greyhounds/racecards/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}$", href):
                meeting_links.add(f"https://www.sportinglife.com{href}")
        
        print(f"    Found {len(meeting_links)} greyhound meetings")
        
        # For each meeting, try to get race info (limit to first few to avoid timeout)
        for meeting_url in sorted(list(meeting_links)[:3]):  # Limit to 3 meetings
            meeting_html = fetch_with_all_methods(meeting_url)
            if meeting_html:
                meeting_races = parse_greyhound_meeting(meeting_html, meeting_url)
                races.extend(meeting_races)
                
    except Exception as e:
        print(f"    ⚠️ Error parsing Sporting Life: {str(e)[:100]}")
    
    return races

def parse_greyhound_meeting(html_content, meeting_url):
    """Parse individual greyhound meeting"""
    races = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract track and date from URL
        url_match = re.search(r"/greyhounds/racecards/([a-z0-9\-]+)/([\d]{4}-[\d]{2}-[\d]{2})", meeting_url)
        if not url_match:
            return races
            
        track_slug, date_str = url_match.groups()
        track = track_slug.replace("-", " ").title()
        
        # Look for race links
        race_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.match(r"^/greyhounds/racecards/[a-z0-9\-]+/\d{4}-\d{2}-\d{2}/\d{3,4}$", href):
                race_links.add(f"https://www.sportinglife.com{href}")
        
        # Process first few races from this meeting
        for race_url in sorted(list(race_links)[:2]):  # Limit to 2 races per meeting
            time_match = re.search(r"/(\d{3,4})$", race_url)
            if time_match:
                time_raw = time_match.group(1)
                if len(time_raw) == 3:
                    time_raw = "0" + time_raw
                race_time = f"{time_raw[:2]}:{time_raw[2:]}"
                
                details = f"[{track.upper()}] {date_str} {race_time} - Greyhound Race"
                
                race = RaceInfo(
                    source="SportingLife",
                    track=track,
                    date=date_str,
                    time=race_time,
                    details=details,
                    runner_count=6,  # Typical greyhound field
                    racecard_url=race_url,
                    result_url=None,
                    discipline="greyhound",
                    country="GB"
                )
                races.append(race)
                
    except Exception as e:
        print(f"    ⚠️ Error parsing greyhound meeting: {str(e)[:100]}")
    
    return races

def parse_harness_races(html_content, source_url, country="AU"):
    """Parse harness racing (AU or CA)"""
    races = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for meeting or race links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "meeting" in href.lower() or "race" in href.lower() or "entries" in href.lower():
                if not href.startswith("http"):
                    href = urljoin(source_url, href)
                links.add(href)
        
        print(f"    Found {len(links)} potential harness links")
        
        # Process a few links
        for link in sorted(list(links)[:2]):  # Limit to 2 to avoid timeout
            link_html = fetch_with_all_methods(link)
            if link_html:
                link_races = parse_harness_meeting(link_html, link, country)
                races.extend(link_races)
                
    except Exception as e:
        print(f"    ⚠️ Error parsing harness races: {str(e)[:100]}")
    
    return races

def parse_harness_meeting(html_content, meeting_url, country):
    """Parse individual harness meeting"""
    races = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract track name
        track = "Harness Track"
        for h in soup.find_all(["h1", "h2", "h3"]):
            text = h.get_text(strip=True)
            if text and len(text.split()) <= 4:  # Likely a track name
                track = text
                break
        
        # Look for time patterns
        text = soup.get_text(" ", strip=True)
        time_matches = re.findall(r'\b(\d{1,2}:\d{2})\b', text)
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        for race_time in time_matches[:3]:  # Limit to 3 races
            details = f"[{track.upper()}] {today} {race_time} - Harness Race ({country})"
            
            race = RaceInfo(
                source=f"Harness{country}",
                track=track,
                date=today,
                time=race_time,
                details=details,
                runner_count=8,  # Typical harness field
                racecard_url=meeting_url,
                result_url=None,
                discipline="harness",
                country=country
            )
            races.append(race)
            
    except Exception as e:
        print(f"    ⚠️ Error parsing harness meeting: {str(e)[:100]}")
    
    return races

def extract_time_from_details(details_text):
    """Extract time from race details"""
    time_match = re.search(r'\b(\d{1,2}:\d{2})\b', details_text)
    return time_match.group(1) if time_match else ""

def parse_race_url_for_info(url):
    """Parse Sky Sports URL for track and date info"""
    try:
        path_parts = urlparse(url).path.strip('/').split('/')
        if 'racecards' in path_parts:
            racecards_index = path_parts.index('racecards')
            if len(path_parts) > racecards_index + 2:
                track = path_parts[racecards_index + 1].replace('-', ' ').title()
                date = path_parts[racecards_index + 2]
                return track, date
    except Exception:
        pass
    return None, None

# Define your strategies (from original script)
STRATEGIES = {
    3: "[Tri000]",
    4: "[FvP, X22, T234, S0000]",
    5: "[XT144]",
    6: "[tbd6]",
    7: "[tbd7]",
}

def generate_enhanced_html(all_races, filtered_races, page_title):
    """Generate enhanced HTML with all sources and filtered results"""
    
    # Group races by source
    races_by_source = {}
    for race in all_races:
        if race.source not in races_by_source:
            races_by_source[race.source] = []
        races_by_source[race.source].append(race)
    
    html_start = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{page_title}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; background-color: #f4f4f9; color: #333; margin: 0; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
        h1 {{ color: #1a1a1a; border-bottom: 3px solid #007bff; padding-bottom: 10px; }}
        h2 {{ color: #333; margin-top: 30px; padding: 10px; background: #f8f9fa; border-left: 4px solid #007bff; }}
        h3.runner-count-header {{ color: yellow; background-color: black; margin-top: 30px; margin-bottom: 15px; padding: 10px; text-align: center; }}
        .source-section {{ margin-bottom: 40px; border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; }}
        .source-header {{ font-size: 1.2em; font-weight: bold; color: #495057; margin-bottom: 15px; padding: 8px; background: #e9ecef; border-radius: 4px; }}
        .race-entry {{ border: 1px solid #ddd; padding: 12px; margin-bottom: 10px; border-radius: 5px; background-color: #fafafa; }}
        .race-details {{ font-weight: bold; font-size: 1.0em; color: #333; margin-bottom: 8px; }}
        .race-meta {{ font-size: 0.9em; color: #666; margin-bottom: 8px; }}
        .race-links a {{ display: inline-block; text-decoration: none; background-color: #007bff; color: white; padding: 6px 12px; border-radius: 4px; margin-right: 8px; font-size: 0.9em; }}
        .race-links a.result-link {{ background-color: #28a745; }}
        .race-links a:hover {{ background-color: #0056b3; }}
        .summary {{ background: #e7f3ff; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .footer {{ text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }}
        .tab-container {{ margin-bottom: 30px; }}
        .tabs {{ display: flex; border-bottom: 2px solid #dee2e6; }}
        .tab {{ padding: 10px 20px; cursor: pointer; background: #f8f9fa; border: 1px solid #dee2e6; border-bottom: none; margin-right: 2px; }}
        .tab.active {{ background: #007bff; color: white; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
    </style>
    <script>
        function showTab(tabName) {{
            // Hide all tab contents
            document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            
            // Show selected tab
            document.getElementById(tabName).classList.add('active');
            document.querySelector(`[onclick="showTab('${{tabName}}')"]`).classList.add('active');
        }}
    </script>
</head>
<body>
    <div class="container">
        <h1>{page_title}</h1>
        
        <div class="summary">
            <strong>📊 Summary:</strong> Found {len(all_races)} total races from {len(races_by_source)} sources. 
            {len(filtered_races)} races match filter criteria (4-6 runners).
        </div>
        
        <div class="tab-container">
            <div class="tabs">
                <div class="tab active" onclick="showTab('filtered')">🎯 Filtered Races (4-6 Runners)</div>
                <div class="tab" onclick="showTab('all')">🏁 All Races by Source</div>
            </div>
            
            <div id="filtered" class="tab-content active">
"""

    # Filtered races section
    html_content = ""
    if filtered_races:
        last_runner_count = None
        for race in filtered_races:
            current_runner_count = race.runner_count
            
            if current_runner_count != last_runner_count:
                if last_runner_count is not None:
                    html_content += '                <hr style="margin: 30px 0; border: 1px solid #dee2e6;">\n'
                
                strategy_text = ""
                if current_runner_count in STRATEGIES:
                    strategy_text = f" {STRATEGIES[current_runner_count]}"
                
                html_content += f'                <h3 class="runner-count-header">Races with {current_runner_count} Runners{strategy_text}</h3>\n'
                last_runner_count = current_runner_count
            
            html_content += f"""                <div class="race-entry">
                    <p class="race-details">{race.details}</p>
                    <p class="race-meta">🏁 {race.discipline.title()} • 📡 {race.source}</p>
                    <div class="race-links">
                        <a href="{race.racecard_url}" target="_blank">Racecard</a>"""
            
            if race.result_url:
                html_content += f'                        <a href="{race.result_url}" target="_blank" class="result-link">Result</a>'
            
            html_content += """                    </div>
                </div>
"""
    else:
        html_content += "                <p>No races found matching the filter criteria (4-6 runners).</p>\n"
    
    html_content += """            </div>
            
            <div id="all" class="tab-content">
"""

    # All races by source section
    for source_name, source_races in races_by_source.items():
        html_content += f"""                <div class="source-section">
                    <div class="source-header">📡 {source_name} ({len(source_races)} races)</div>
"""
        
        for race in source_races:
            html_content += f"""                    <div class="race-entry">
                        <p class="race-details">{race.details}</p>
                        <p class="race-meta">🏁 {race.discipline.title()} • 👥 {race.runner_count if race.runner_count != 999 else '?'} runners</p>
                        <div class="race-links">
                            <a href="{race.racecard_url}" target="_blank">Racecard</a>"""
            
            if race.result_url:
                html_content += f'                            <a href="{race.result_url}" target="_blank" class="result-link">Result</a>'
            
            html_content += """                        </div>
                    </div>
"""
        
        html_content += "                </div>\n"
    
    html_content += """            </div>
        </div>
"""

    html_end = f"""
        <div class="footer">
            <p>Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")} | Enhanced Multi-Source Racing Scanner</p>
        </div>
    </div>
    <script>
        // Initialize with filtered tab active
        document.addEventListener('DOMContentLoaded', function() {{
            showTab('filtered');
        }});
    </script>
</body>
</html>
"""
    
    return html_start + html_content + html_end

def fetch_all_sources():
    """Fetch races from all configured data sources"""
    all_races = []
    today = datetime.now()
    
    print("\n" + "="*80)
    print("🏁 ENHANCED MULTI-SOURCE RACING SCANNER")
    print("="*80)
    
    for source in DATA_SOURCES:
        print(f"\n📡 Processing {source.name} ({source.discipline}, {source.country})")
        print("-" * 60)
        
        try:
            urls = source.get_urls_for_date(today)
            if not urls:
                urls = [source.base_url]
            
            source_races = []
            
            for url in urls:
                print(f"  🔍 Fetching: {url}")
                
                html_content = fetch_with_all_methods(url)
                if not html_content:
                    print(f"    ❌ Failed to fetch content")
                    continue
                
                print(f"    ✅ Got content ({len(html_content):,} chars)")
                
                # Parse based on source type
                if source.name == "SkyRacing":
                    races = parse_sky_sports_races(html_content, url)
                elif source.name == "AtTheRaces":
                    # Extract region from URL
                    region_match = re.search(r'/tabs/([^/]+)/', url)
                    region = region_match.group(1) if region_match else "uk"
                    races = parse_atr_races(html_content, url, region)
                elif source.name == "SportingLife":
                    races = parse_sporting_life_greyhounds(html_content, url)
                elif source.name == "HarnessAustralia":
                    races = parse_harness_races(html_content, url, "AU")
                elif source.name == "StandardbredCanada":
                    races = parse_harness_races(html_content, url, "CA")
                else:
                    races = []
                
                source_races.extend(races)
                print(f"    📝 Parsed {len(races)} races")
                
                # Small delay between requests to be respectful
                time.sleep(1)
            
            print(f"  🎯 Total from {source.name}: {len(source_races)} races")
            all_races.extend(source_races)
            
        except Exception as e:
            print(f"  ❌ Error processing {source.name}: {str(e)[:100]}")
            continue
    
    return all_races

def filter_races(races, min_runners=4, max_runners=6):
    """Filter races by runner count"""
    filtered = []
    for race in races:
        if min_runners <= race.runner_count <= max_runners:
            filtered.append(race)
    return filtered

def save_enhanced_output_files(all_races, filtered_races):
    """Save the enhanced output files"""
    print("\n" + "="*60)
    print("💾 SAVING OUTPUT FILES")
    print("="*60)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    base_filename = f"enhanced_racing_scanner_{timestamp}"
    
    # Sort filtered races by runner count
    filtered_races.sort(key=lambda r: (r.runner_count, r.source, r.time))
    
    # Generate HTML
    page_title = f"Enhanced Multi-Source Racing Scanner - {len(all_races)} Total Races"
    html_content = generate_enhanced_html(all_races, filtered_races, page_title)
    
    # Save HTML file
    html_filename = f"{base_filename}.html"
    try:
        with open(html_filename, 'w', encoding='utf-8', errors='replace') as f:
            f.write(html_content)
        print(f"✅ Enhanced HTML report saved: {os.path.abspath(html_filename)}")
        
        # Auto-open in browser
        try:
            webbrowser.open(f'file://{os.path.abspath(html_filename)}')
            print("🌐 File opened in browser automatically")
        except Exception as e:
            print(f"⚠️ Could not auto-open browser: {e}")
            
    except Exception as e:
        print(f"❌ Failed to save HTML file: {e}")
    
    # Save JSON data file
    json_filename = f"{base_filename}.json"
    try:
        json_data = {
            "generated_at": datetime.now().isoformat(),
            "total_races": len(all_races),
            "filtered_races": len(filtered_races),
            "sources_used": list(set(race.source for race in all_races)),
            "all_races": [
                {
                    "source": race.source,
                    "track": race.track,
                    "date": race.date,
                    "time": race.time,
                    "details": race.details,
                    "runner_count": race.runner_count,
                    "racecard_url": race.racecard_url,
                    "result_url": race.result_url,
                    "discipline": race.discipline,
                    "country": race.country
                }
                for race in all_races
            ],
            "filtered_races": [
                {
                    "source": race.source,
                    "track": race.track,
                    "date": race.date,
                    "time": race.time,
                    "details": race.details,
                    "runner_count": race.runner_count,
                    "racecard_url": race.racecard_url,
                    "result_url": race.result_url,
                    "discipline": race.discipline,
                    "country": race.country
                }
                for race in filtered_races
            ]
        }
        
        with open(json_filename, 'w', encoding='utf-8', errors='replace') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"✅ JSON data saved: {os.path.abspath(json_filename)}")
        
    except Exception as e:
        print(f"❌ Failed to save JSON file: {e}")
    
    # Print summary
    print(f"\n📊 FINAL SUMMARY:")
    print(f"   🎯 Total races found: {len(all_races)}")
    print(f"   🏆 Filtered races (4-6 runners): {len(filtered_races)}")
    
    if all_races:
        sources_summary = {}
        for race in all_races:
            if race.source not in sources_summary:
                sources_summary[race.source] = {"total": 0, "filtered": 0}
            sources_summary[race.source]["total"] += 1
            if race in filtered_races:
                sources_summary[race.source]["filtered"] += 1
        
        print(f"   📡 Sources breakdown:")
        for source, counts in sources_summary.items():
            print(f"      • {source}: {counts['total']} total, {counts['filtered']} filtered")

def get_url_with_interactive_timeout(timeout=2):
    """Interactive URL input with timeout (Windows optimized)"""
    print(f"🔗 URL Input (Optional)")
    print(f"Press any key within {timeout} seconds to enter a custom URL, or wait to use multi-source mode...")
    
    for i in range(timeout, 0, -1):
        sys.stdout.write(f"\r⏳ Time remaining: {i}s... ")
        sys.stdout.flush()
        if msvcrt.kbhit():
            break
        time.sleep(1)
    
    sys.stdout.write("\r" + " " * 40 + "\r")
    sys.stdout.flush()

    if msvcrt.kbhit():
        # Clear the buffer
        while msvcrt.kbhit(): 
            msvcrt.getch()
        print("📝 Enter your URL:")
        user_input = sys.stdin.readline().strip()
        if user_input:
            print(f"🎯 Using custom URL: {user_input}")
            return user_input
    
    print("🚀 Using multi-source scanning mode")
    return None

def main():
    """Main function"""
    print("🏁 ENHANCED RACING SCANNER")
    print(f"🖥️  Operating System: {platform.system()}")
    print("="*60)
    
    # Check if user wants to use single URL mode or multi-source mode
    custom_url = get_url_with_interactive_timeout(3)
    
    if custom_url:
        # Single URL mode (original behavior)
        print(f"\n🔍 Single URL Mode")
        print(f"Fetching: {custom_url}")
        
        html_content = fetch_with_all_methods(custom_url)
        if html_content:
            print("✅ Content fetched successfully")
            races = parse_sky_sports_races(html_content, custom_url)
            filtered_races = filter_races(races, 4, 6)
            
            if races:
                save_enhanced_output_files(races, filtered_races)
            else:
                print("❌ No races found in the content")
        else:
            print("❌ Failed to fetch content from URL")
    
    else:
        # Multi-source mode (new behavior)
        print(f"\n🌐 Multi-Source Mode")
        
        try:
            all_races = fetch_all_sources()
            filtered_races = filter_races(all_races, 4, 6)
            
            if all_races:
                save_enhanced_output_files(all_races, filtered_races)
            else:
                print("❌ No races found from any source")
                print("💡 This could be due to:")
                print("   • Network connectivity issues")
                print("   • Website structure changes")
                print("   • Rate limiting by websites")
                print("   • No races scheduled for today")
        
        except KeyboardInterrupt:
            print("\n\n⚠️ Scan interrupted by user")
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            print("💡 Try running in single URL mode for debugging")
    
    print("\n🏁 Scan complete!")
    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
