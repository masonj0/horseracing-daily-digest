import requests
import subprocess
import shutil
import platform
import certifi
import trafilatura
import os
import sys
import time
if platform.system() == "Windows":
    import msvcrt
else:
    import select
import re
import webbrowser
import argparse
from datetime import datetime
from urllib.parse import urlparse
from curl_cffi.requests import Session as CurlCffiSession
from bs4 import BeautifulSoup

# Suppress only the single InsecureRequestWarning from urllib3 needed for verify=False
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

DEFAULT_URL = "https://www.skysports.com/racing/racecards"

# ... (The try_curl_cffi, try_requests_with_variations, and try_subprocess_curl functions are unchanged) ...
def try_curl_cffi(url):
    """
    Method 1: The most effective solution for TLS fingerprinting issues.
    It mimics a real browser's TLS handshake to bypass advanced bot detection.
    """
    print("--- Trying Method 1: Browser Impersonation (curl_cffi) ---")
    try:
        session = CurlCffiSession(impersonate="chrome120", timeout=20)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = session.get(url, headers=headers, verify=certifi.where())
        response.raise_for_status()
        print("✅ Success with curl_cffi!")
        return response.text
    except Exception as e:
        print(f"❌ curl_cffi method failed: {e}\n")
        return None

def try_requests_with_variations(url):
    """
    Method 2: Tries standard requests with several URL variations and disabled SSL verification.
    """
    print("--- Trying Method 2: Requests with URL Variations & No SSL Verify ---")

    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace('m.', '').replace('www.', '')

    url_variations = {
        url,
        f"https://www.{domain}{parsed_url.path}",
        f"https://{domain}{parsed_url.path}",
        f"http://{domain}{parsed_url.path}",
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }

    for i, test_url in enumerate(url_variations):
        print(f"  -> Attempt {i+1}: Trying {test_url}")
        try:
            response = requests.get(test_url, headers=headers, verify=False, timeout=15, allow_redirects=True)
            if response.status_code == 200 and len(response.text) > 500:
                print(f"✅ Success with Requests on URL: {test_url}")
                return response.text
        except requests.exceptions.RequestException as e:
            print(f"     Failed: {e}")
            continue

    print("❌ All Requests variations failed.\n")
    return None

def try_subprocess_curl(url):
    """
    Method 3: The last resort, using the system's 'curl' command.
    """
    print("--- Trying Method 3: Subprocess curl (Last Resort) ---")

    if not shutil.which("curl"):
        print("❌ 'curl' command not found in system PATH. Skipping this method.")
        return None

    try:
        curl_cmd = [
            'curl', '--silent', '--show-error', '--location', '--compressed',
            '--max-time', '30', '--connect-timeout', '10',
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            '--header', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            '--ssl-no-revoke', url
        ]
        result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=45, check=False)

        if result.returncode == 0 and result.stdout:
            print("✅ Success with subprocess curl!")
            return result.stdout
        else:
            error_message = result.stderr.strip() if result.stderr else f"Curl exited with code {result.returncode}"
            print(f"❌ Subprocess curl failed: {error_message}\n")
            return None

    except Exception as e:
        print(f"❌ Subprocess curl method failed with an exception: {e}\n")
        return None

def extract_runner_count(details_text):
    """
    A helper function to find the number of runners in a text block.
    Uses regex to find a number followed by 'runner' or 'runners'.
    Returns the number as an integer.
    """
    match = re.search(r'(\d+)\s+runners?', details_text)
    if match:
        return int(match.group(1))
    return 999 # Return a high number if not found

def parse_race_url_for_info(url):
    """
    Parses a Sky Sports racecard URL to extract the track and date.
    Example: .../racecards/ffos-las/16-07-2025/... -> ('Ffos Las', '16-07-2025')
    Returns (track, date) or (None, None) if parsing fails.
    """
    try:
        path_parts = urlparse(url).path.strip('/').split('/')
        if 'racecards' in path_parts:
            racecards_index = path_parts.index('racecards')
            if len(path_parts) > racecards_index + 2:
                track = path_parts[racecards_index + 1].replace('-', ' ').title()
                date = path_parts[racecards_index + 2]
                return track, date
    except Exception:
        # Fails silently if the URL format is unexpected
        pass
    return None, None

# Define your strategies here
# Keys are runner counts (integers), values are the strategy strings
STRATEGIES = {
    3: "[Tri000]",
    4: "[FvP, X22, T234, S0000]",
    5: "[XT144]",
    6: "[tbd6]",
    7: "[tbd7]",
    # Add more as needed
}

def generate_filtered_html(races, page_title):
    """
    Generates a clean, clickable HTML file from a list of race data dictionaries.
    It now adds a horizontal separator and a runner-count display above
    each grouping of races with different runner counts, including a strategy.
    """
    html_start = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{page_title}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; background-color: #f4f4f9; color: #333; margin: 0; padding: 20px; }}
        .container {{ max-width: 800px; margin: 0 auto; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
        h1 {{ color: #1a1a1a; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        h2.runner-count-header {{ color: yellow; background-color: black; margin-top: 40px; margin-bottom: 15px; padding-bottom: 5px; border-bottom: 1px solid #eee; text-align: center; }}
        .race-entry {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fafafa; }}
        .race-details {{ font-weight: bold; font-size: 1.1em; color: #333; margin-bottom: 10px; }}
        .race-links a {{ display: inline-block; text-decoration: none; background-color: #007bff; color: white; padding: 8px 15px; border-radius: 4px; margin-right: 10px; font-weight: bold; transition: background-color 0.2s; }}
        .race-links a.result-link {{ background-color: #28a745; }}
        .race-links a:hover {{ background-color: #0056b3; }}
        .race-links a.result-link:hover {{ background-color: #218838; }}
        .footer {{ text-align: center; margin-top: 20px; font-size: 0.9em; color: #777; }}
        .group-separator {{ border: 0; height: 1px; background-image: linear-gradient(to right, rgba(0, 0, 0, 0), rgba(0, 0, 0, 0.25), rgba(0, 0, 0, 0)); margin: 75px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{page_title}</h1>
"""

    html_content = ""
    last_runner_count = None # Initialize to None to ensure the first group gets a header

    for race in races:
        current_runner_count = race.get('runner_count', 'N/A')

        # --- LOGIC TO ADD SEPARATOR AND RUNNER COUNT HEADER ---
        if current_runner_count != last_runner_count:
            # Add a separator if it's not the very first group
            if last_runner_count is not None:
                html_content += '        <hr class="group-separator">\n'

            # Add the runner-count display with strategy
            strategy_text = ""
            if isinstance(current_runner_count, int) and current_runner_count in STRATEGIES:
                strategy_text = f" {STRATEGIES[current_runner_count]}"

            html_content += f'        <h2 class="runner-count-header">Races with {current_runner_count} Runners{strategy_text}</h2>\n'
            last_runner_count = current_runner_count

        html_content += '        <div class="race-entry">\n'
        html_content += f'            <p class="race-details">{race["details_text"]}</p>\n'
        html_content += '            <div class="race-links">\n'
        if race.get("racecard_url"):
            html_content += f'                <a href="{race["racecard_url"]}" target="_blank">Racecard</a>\n'
        if race.get("result_url"):
            html_content += f'                <a href="{race["result_url"]}" target="_blank" class="result-link">Result</a>\n'
        html_content += '            </div>\n'
        html_content += '        </div>\n'

    html_end = f"""
        <div class="footer">
            <p>Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p>
        </div>
    </div>
</body>
</html>
"""
    return html_start + html_content + html_end

def _save_raw_html(soup, base_filename, html_content):
    """Saves the raw HTML body content to a text file."""
    raw_filename = f"{base_filename}_raw.txt"
    try:
        body_content = soup.body.prettify() if soup.body else html_content
        with open(raw_filename, 'w', encoding='utf-8') as f:
            f.write(body_content)
        print(f"✅ Raw HTML (body content) saved to: {os.path.abspath(raw_filename)}")
    except Exception as e:
        print(f"❌ Failed to write raw HTML file: {e}")

def _save_reader_text(html_content, base_filename):
    """Saves a reader-friendly version of the content using Trafilatura."""
    reader_filename = f"{base_filename}_reader.txt"
    try:
        reader_text = trafilatura.extract(html_content) or "Trafilatura could not extract main content."
        with open(reader_filename, 'w', encoding='utf-8') as f:
            f.write(reader_text)
        print(f"✅ Reader view saved to: {os.path.abspath(reader_filename)}")
    except Exception as e:
        print(f"❌ Failed to write reader view file: {e}")

def _extract_race_data(soup, parsed_url_obj):
    """Parses the BeautifulSoup object to find and structure race data."""
    all_race_data = []
    event_containers = soup.find_all('div', class_='sdc-site-racing-meetings__event')

    for container in event_containers:
        result_tag = container.find('a', class_='sdc-site-racing-meetings__event-result')
        racecard_tag = container.find('a', class_='sdc-site-racing-meetings__event-link')

        if not (racecard_tag and racecard_tag.get('href')):
            continue

        racecard_url = f"{parsed_url_obj.scheme}://{parsed_url_obj.netloc}{racecard_tag.get('href')}"
        result_url = f"{parsed_url_obj.scheme}://{parsed_url_obj.netloc}{result_tag.get('href')}" if result_tag and result_tag.get('href') else None

        race_name_span = container.find('span', class_='sdc-site-racing-meetings__event-name')
        race_details_span = container.find('span', class_='sdc-site-racing-meetings__event-details')
        details_parts = [span.get_text(strip=True) for span in [race_name_span, race_details_span] if span]
        original_details_text = ' '.join(details_parts)

        if not original_details_text:
            continue

        track, race_date = parse_race_url_for_info(racecard_url)
        full_details_text = f"[{track.upper()}] {race_date} - {original_details_text}" if track and race_date else original_details_text

        runner_count = extract_runner_count(full_details_text)

        all_race_data.append({
            "details_text": full_details_text,
            "racecard_url": racecard_url,
            "result_url": result_url,
            "runner_count": runner_count
        })

    if not event_containers:
        print("⚠️ Could not find the main event containers ('sdc-site-racing-meetings__event'). The page structure may have changed.")

    return all_race_data

def _save_all_links_file(all_races, base_filename):
    """Saves a text file with details for all extracted races."""
    links_filename = f"{base_filename}_race_links.txt"

    all_race_data_text = []
    for race in all_races:
        text_block_lines = []
        if race.get("result_url"): text_block_lines.append(f"Result: {race['result_url']}")
        text_block_lines.append(f"URL: {race['racecard_url']}")
        text_block_lines.append(f"Details: {race['details_text']}")
        all_race_data_text.append("\n".join(text_block_lines))

    with open(links_filename, 'w', encoding='utf-8') as f:
        if all_race_data_text:
            f.write(("\n" + "-"*40 + "\n").join(all_race_data_text))
            print(f"✅ Extracted {len(all_race_data_text)} total race entries. Saved to: {os.path.abspath(links_filename)}")
        else:
            f.write("No race entries matching the expected pattern were found on the page.")
            print("⚠️ No race entries matching the expected pattern were found.")

def _generate_and_save_filtered_report(races, min_runners, max_runners, domain, base_filename):
    """Filters races by runner count, generates an HTML report, and opens it."""
    filtered_races = [r for r in races if min_runners <= r['runner_count'] <= max_runners]

    if not filtered_races:
        print(f"ℹ️ No races matching the filter ({min_runners}-{max_runners} runners) were found.")
        return

    print(f"...Sorting {len(filtered_races)} filtered races by field size...")
    filtered_races.sort(key=lambda item: item['runner_count'])

    page_title = f"Filtered Races ({min_runners}-{max_runners} Runners) from {domain}"
    html_output = generate_filtered_html(filtered_races, page_title)

    filtered_filename = f"{base_filename}_filtered_races.htm"
    with open(filtered_filename, 'w', encoding='utf-8') as f:
        f.write(html_output)
    print(f"✅ BONUS: Found and saved {len(filtered_races)} small-field races (sorted) to a clickable HTML file: {os.path.abspath(filtered_filename)}")

    try:
        print("... Automatically opening the HTML file in your browser...")
        webbrowser.open(f'file://{os.path.abspath(filtered_filename)}')
    except Exception as e:
        print(f"⚠️ Could not automatically open the file in a browser: {e}")


def save_output_files(html_content, url, save_raw, save_reader, save_all_links, min_runners, max_runners):
    """
    Orchestrates the parsing of HTML and saving of output files based on command-line arguments.
    """
    print("\n--- Generating output files ---")

    parsed_url_obj = urlparse(url)
    domain = parsed_url_obj.netloc.replace('www.', '')
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    base_filename = f"{domain}_{timestamp}"

    soup = BeautifulSoup(html_content, 'html.parser')

    if save_raw:
        _save_raw_html(soup, base_filename, html_content)

    if save_reader:
        _save_reader_text(html_content, base_filename)

    print("...Parsing for all racecard, result, and detail information...")
    try:
        all_races = _extract_race_data(soup, parsed_url_obj)

        if not all_races:
            print("ℹ️ No race data could be extracted from the page.")
            return

        if save_all_links:
            _save_all_links_file(all_races, base_filename)

        _generate_and_save_filtered_report(all_races, min_runners, max_runners, domain, base_filename)

    except Exception as e:
        print(f"❌ Failed during race data processing: {e}")

def get_url_with_interactive_timeout(timeout=2):
    """
    Prompts for a URL with a visible countdown.
    Proceeds automatically if the timer runs out.
    Cross-platform implementation.
    """
    print(f"Enter a URL or paste one now. Will use default in {timeout} seconds...")
    print(f"Default URL: {DEFAULT_URL}")

    if platform.system() == "Windows":
        # Original Windows implementation with countdown
        for i in range(timeout, 0, -1):
            sys.stdout.write(f"\r> Time remaining: {i}s... ")
            sys.stdout.flush()
            if msvcrt.kbhit():
                break
            time.sleep(1)

        sys.stdout.write("\r" + " " * 40 + "\r")
        sys.stdout.flush()

        if msvcrt.kbhit():
            # Clear the buffer in case of pasted text with newlines
            while msvcrt.kbhit(): msvcrt.getch()
            print("> Input detected. Please enter your URL:")
            user_input = sys.stdin.readline().strip()
            if user_input:
                print(f"Using custom URL: {user_input}")
                return user_input
    else:
        # Non-Windows (Linux, macOS) implementation using select
        print(f"> You have {timeout} seconds to enter a URL:")
        rlist, _, _ = select.select([sys.stdin], [], [], timeout)
        if rlist:
            user_input = sys.stdin.readline().strip()
            if user_input:
                print(f"Using custom URL: {user_input}")
                return user_input

    print("⏰ Timeout or empty input! Using the default URL.")
    return DEFAULT_URL

def main():
    parser = argparse.ArgumentParser(
        description="A script to scrape racecard data from Sky Sports.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  # Run with interactive URL prompt and default filters (3-4 runners)
  python sky_sports_scraper.py

  # Scrape a specific URL and save all optional files
  python sky_sports_scraper.py https://www.skysports.com/racing/racecards --save-raw --save-reader --save-all-links

  # Scrape and filter for races with 5 to 8 runners
  python sky_sports_scraper.py --min-runners 5 --max-runners 8
"""
    )
    parser.add_argument('url', nargs='?', default=None,
                        help='URL to scrape. If not provided, an interactive prompt will appear.')
    parser.add_argument('--save-raw', action='store_true',
                        help='Save the raw HTML of the page.')
    parser.add_argument('--save-reader', action='store_true',
                        help='Save a simplified "reader" version of the page using Trafilatura.')
    parser.add_argument('--save-all-links', action='store_true',
                        help='Save a text file with all found race links.')
    parser.add_argument('--min-runners', type=int, default=3,
                        help='The minimum number of runners to include in the filtered HTML report.')
    parser.add_argument('--max-runners', type=int, default=4,
                        help='The maximum number of runners to include in the filtered HTML report.')
    parser.add_argument('--timeout', type=int, default=2,
                        help='Timeout in seconds for the interactive URL prompt.')

    args = parser.parse_args()

    if args.url:
        url = args.url
        print(f"Using URL from command line: {url}")
    else:
        url = get_url_with_interactive_timeout(args.timeout)

    if not url:
        # This case handles an empty string from the command line, e.g. python script.py ""
        # The interactive prompt is designed to always return a usable URL.
        print("No URL provided, using default.")
        url = DEFAULT_URL

    print(f"\nFetching source code from: {url}")
    print(f"Operating System: {platform.system()}")
    print("=" * 60)

    source_code = try_curl_cffi(url)

    if not source_code:
        source_code = try_requests_with_variations(url)

    if not source_code:
        source_code = try_subprocess_curl(url)

    print("-" * 60)
    if source_code:
        print("🎉 SUCCESS! Raw source code retrieved.")
        save_output_files(
            source_code,
            url,
            save_raw=args.save_raw,
            save_reader=args.save_reader,
            save_all_links=args.save_all_links,
            min_runners=args.min_runners,
            max_runners=args.max_runners
        )
    else:
        print("💔 All methods failed to retrieve the source code.")
        print("This website has very strong protection. For such cases, using a full")
        print("browser automation tool like Selenium might be the only remaining option.")

if __name__ == "__main__":
    main()
