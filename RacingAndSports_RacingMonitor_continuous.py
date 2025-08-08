#!/usr/bin/env python3
"""
Horse Racing Discipline Summary Generator for RacingAndSports.com.au (v14.0)
Fetches the main R&S JSON and creates a collapsible HTML summary for ALL
meetings, using a multi-tier fallback system and a new filename format.
"""

import requests
import json
import time
from datetime import datetime
import os
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RacingAndSportsFetcher:
    def __init__(self, api_url, output_dir=""):
        """
        Initialize the fetcher.
        
        Args:
            api_url (str): The API endpoint URL
            output_dir (str): Directory to save output files (set to "" for same directory)
        """
        self.api_url = api_url
        self.output_dir = output_dir
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = self.create_session()
    
    def create_session(self):
        """Create a requests session."""
        session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.racingandsports.com.au/todays-racing',
        }
        session.headers.update(headers)
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session
    
    def fetch_data(self):
        """Fetches the main JSON directory of all meetings."""
        print("Fetching main meeting directory...")
        try:
            response = self.session.get(self.api_url, timeout=30, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Could not fetch main JSON directory: {e}")
        except json.JSONDecodeError:
            print("ERROR: Failed to decode main directory. Response was not valid JSON.")
        return None

    def process_meetings_data(self, json_data):
        """
        Processes the main JSON data to get a list of ALL meetings,
        using a multi-tier fallback system and appending a suffix for better links.
        """
        if not isinstance(json_data, list):
            print("ERROR: Expected a JSON list, but received a different data type.")
            return None

        print("Processing all meeting data (active and closed)...")
        processed_meetings = []
        for discipline_data in json_data:
            discipline_name = discipline_data.get("DisciplineFullText", "Unknown")
            for country_data in discipline_data.get("Countries", []):
                country_name = country_data.get("CountryName", "Unknown")
                for meeting_data in country_data.get("Meetings", []):
                    course_name = meeting_data.get("Course")
                    final_link = None

                    # --- Two-Tier Fallback Logic (Revised based on user feedback) ---
                    
                    # Tier 1: The best link is always the direct PDF, if it exists.
                    pdf_url = meeting_data.get("PDFUrl")
                    if pdf_url and pdf_url.strip():
                        final_link = pdf_url
                    
                    # Tier 2: If no PDF, use the PreMeetingUrl and append the suffix.
                    else:
                        # Corrected the key from 'PreMeetingURL' to 'PreMeetingUrl'.
                        pre_meeting_url = meeting_data.get("PreMeetingUrl")
                        if pre_meeting_url:
                            # USER REQUEST: Append '/race-fields' for a better landing page.
                            final_link = pre_meeting_url + '/race-fields'
                            print(f"  -> NOTE: No PDF for '{course_name}'. Using PreMeetingUrl + suffix.")
                        else:
                            # This case is unlikely given the data, but is a safe final fallback.
                             print(f"  -> WARNING: No usable URLs found for '{course_name}'.")

                    if course_name:
                        processed_meetings.append({
                            'discipline': discipline_name,
                            'country': country_name,
                            'course': course_name,
                            'pdf_link': final_link,
                        })
        return processed_meetings

    def generate_summary_html(self, meetings_data):
        """
        Generates a collapsible HTML summary with direct links to live form guides.
        """
        if not meetings_data:
            return "<html><body><h1>No meetings found in the JSON data.</h1></body></html>"
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        meetings_by_group = {}
        for meeting in meetings_data:
            meetings_by_group.setdefault(meeting['discipline'], {}).setdefault(meeting['country'], []).append(meeting)

        html_lines = [
            "<!DOCTYPE html>", "<html lang='en'>", "<head>",
            "<meta charset='UTF-8'>", f"<title>All Racing Meetings - {timestamp}</title>",
            "<style>",
            "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; background: #f4f4f4; color: #333; margin: 0; padding: 20px; }",
            ".container { max-width: 800px; margin: auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
            "h1 { text-align: center; border-bottom: 2px solid #eee; padding-bottom: 10px; }",
            "h1, h2, h3 { color: #0056b3; margin: 0; padding: 0; display: inline-block; }",
            "summary { cursor: pointer; outline: none; padding: 10px; font-weight: bold; }",
            "summary::-webkit-details-marker, summary::marker { display: none; } summary { list-style: none; }",
            "details > summary:before { content: '+ '; } details[open] > summary:before { content: '− '; }",
            ".discipline-details > summary { background-color: #e7f3ff; border-left: 5px solid #0056b3; margin-top: 25px; }",
            ".country-details > summary { margin-top: 10px; }",
            ".discipline-content, .country-content { padding-left: 25px; }",
            "ul { list-style-type: none; padding-left: 0; margin-top: 10px; }",
            "li { background: #fff; margin-bottom: 8px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }",
            "a { text-decoration: none; color: #007bff; font-weight: bold; } a:hover { text-decoration: underline; }",
            ".no-link { color: #6c757d; }",
            "</style>", "</head>", "<body>", "<div class='container'>",
            f"<h1>All Horse Racing Meetings</h1><p style='text-align:center;'>Generated: {timestamp}</p>",
        ]

        for discipline, countries in sorted(meetings_by_group.items()):
            html_lines.append('<details class="discipline-details" open><summary><h2>' + discipline.upper() + '</h2></summary><div class="discipline-content">')
            for country, meetings in sorted(countries.items()):
                html_lines.append('<details class="country-details" open><summary><h3>' + country + '</h3></summary><div class="country-content"><ul>')
                for meeting in sorted(meetings, key=lambda m: m['course']):
                    course = meeting['course']
                    pdf_link = meeting.get('pdf_link')
                    
                    if pdf_link:
                        html_lines.append(f'<li><a href="{pdf_link}" target="_blank" title="View form for {course} on Racing & Sports">{course}</a></li>')
                    else:
                        html_lines.append(f'<li><span class="no-link">{course} (Form Guide link not available)</span></li>')

                html_lines.append("</ul></div></details>")
            html_lines.append("</div></details>")

        html_lines.extend(["</div>", "</body>", "</html>"])
        return "\n".join(html_lines)

    def save_html_summary(self, html_content):
        """Saves the HTML summary to a file with the new naming convention."""
        # NEW: Changed timestamp format and filename structure.
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = f"racingandsports_formguides_{timestamp_str}.htm"
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"\nHTML summary saved to: {filepath}")
        except Exception as e:
            print(f"Error saving HTML summary: {e}")

    def run_once(self):
        """Fetches data and generates the summary file once."""
        print(f"--- Starting run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        json_data = self.fetch_data()
        if not json_data:
            return False
        
        meetings_data = self.process_meetings_data(json_data)
        if meetings_data is None:
            return False

        html_summary = self.generate_summary_html(meetings_data)
        self.save_html_summary(html_summary)
        
        if meetings_data:
             print(f"\n--- Run complete. Processed {len(meetings_data)} meetings. ---")
        return True

    def run_continuously(self):
        """Runs the fetcher interactively with user prompts."""
        print("=" * 60)
        try:
            while True:
                self.run_once()
                print("\n" + "="*50)
                user_input = input("\nPress 'G' + Enter to fetch again, or just Enter to quit: ").strip().upper()
                if user_input != 'G':
                    print("Goodbye!"); break
        except KeyboardInterrupt:
            print("\n\nStopping fetcher...")
        except Exception as e:
            print(f"A critical unexpected error occurred: {e}")

def main():
    API_URL = "https://www.racingandsports.com.au/todays-racing-json-v2"
    OUTPUT_DIR = "" # Current directory
    
    print("RacingAndSports.com.au All Meetings Linker (v14.0)")
    print("This script creates a collapsible HTML index of all meetings for the day,")
    print("using a multi-tier fallback system to find the best link.")
    print("-" * 60)
    
    fetcher = RacingAndSportsFetcher(API_URL, OUTPUT_DIR)
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        sys.exit(0 if fetcher.run_once() else 1)
    else:
        fetcher.run_continuously()

if __name__ == "__main__":
    main()
