#!/usr/bin/env python3
"""
Horse Racing Data Fetcher
Fetches race data from Sporting Life API and outputs simple text summaries.
"""

import requests
import json
import time
from datetime import datetime
import os
import re
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class HorseRacingFetcher:
    def __init__(self, api_url, fetch_interval=300, output_dir=""):
        """
        Initialize the fetcher.
        
        Args:
            api_url (str): The API endpoint URL
            fetch_interval (int): Time between fetches in seconds (default: 5 minutes)
            output_dir (str): Directory to save output files
        """
        self.api_url = api_url
        self.fetch_interval = fetch_interval
        self.output_dir = output_dir
        
        # Disable SSL warnings (optional - comment out if you want to see SSL warnings)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Setup session with browser-like headers and retry strategy
        self.session = self.create_session()
    
    def create_session(self):
        """Create a requests session configured to appear like a browser."""
        session = requests.Session()
        
        # --- START OF CHANGES ---
        # More comprehensive browser-like headers to bypass modern WAF/bot detection
        # The key is to look like a request originating from the website itself.
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://www.sportinglife.com',
            'Referer': 'https://www.sportinglife.com/horse-racing/racecards',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            # Adding modern "Client Hints" headers that Chrome sends
            'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
        }
        # --- END OF CHANGES ---

        session.headers.update(headers)
        
        # Setup retry strategy (no changes here, but it's still good to have)
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def fetch_race_data(self):
        """Fetch race data from the API."""
        # We will simplify this. The ConnectionResetError is not an SSL issue.
        # The main problem is the request fingerprint. We will rely on our improved session.
        try:
            print("Attempting to fetch data with enhanced browser headers...")
            response = self.session.get(self.api_url, timeout=30, verify=False) # Keep verify=False as it helps with some network issues
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Fetch failed: {e}")
            # The original error was a ConnectionResetError, which is a subclass of RequestException.
            # This will catch it and provide a more helpful message.
            if isinstance(e, requests.exceptions.ConnectionError):
                print("\nSuggestion: The server is actively closing the connection.")
                print("This is likely due to anti-bot measures. The updated headers may not be enough.")
                print("The server may have temporarily blocked your IP address.")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during fetch: {e}")
            return None

    # NOTE: The _fetch_... fallback methods are removed as they are unlikely to help with a ConnectionResetError
    # and simplifying the logic makes the code cleaner. The main `fetch_race_data` now handles the attempt.
    
    def parse_odds(self, odds_string):
        """Parse odds string to extract numerical odds."""
        if not odds_string:
            return None
        
        # Handle fractional odds like "3/1", "7/2", etc.
        if '/' in odds_string:
            try:
                num, den = odds_string.split('/')
                return f"{num}/{den}"
            except:
                return odds_string
        return odds_string
    
    def get_top_two_favorites(self, rides):
        """Get the top two favorites based on current odds."""
        # Filter out non-runners and sort by odds
        runners = [ride for ride in rides if ride.get('ride_status') == 'RUNNER']
        
        # Convert odds to sortable format (lower fractional odds = more favored)
        def odds_to_decimal(odds_str):
            if not odds_str or 'SP' in odds_str.upper():
                return float('inf')
            try:
                if '/' in odds_str:
                    num, den = map(float, odds_str.split('/'))
                    if den == 0: return float('inf')
                    return (num / den) + 1
                else:
                    return float('inf')
            except (ValueError, TypeError):
                return float('inf')
        
        # Sort by odds (lower decimal = more favored)
        sorted_runners = sorted(runners, key=lambda x: odds_to_decimal(x.get('betting', {}).get('current_odds', '')))
        
        top_two = []
        for i, runner in enumerate(sorted_runners[:2]):
            horse_name = runner.get('horse', {}).get('name', 'Unknown')
            odds = runner.get('betting', {}).get('current_odds', 'N/A')
            top_two.append(f"{i+1}. {horse_name} ({odds})")
        
        return top_two
    
    def process_race_data(self, race_data):
        """Process the raw race data and extract key information."""
        if not race_data:
            return None
        
        processed_races = []
        
        for race in race_data[:10]:  # Limit to 10 races as requested
            race_summary = race.get('race_summary', {})
            rides = race.get('rides', [])
            
            # Extract basic race info
            race_name = race_summary.get('name', 'Unknown Race')
            course_name = race_summary.get('course_name', 'Unknown Course')
            race_time = race_summary.get('time', 'Unknown Time')
            race_date = race_summary.get('date', 'Unknown Date')
            distance = race_summary.get('distance', 'Unknown Distance')
            
            # Count runners (excluding non-runners)
            total_entries = len(rides)
            runners = len([r for r in rides if r.get('ride_status') == 'RUNNER'])
            non_runners = total_entries - runners
            
            # Get top two favorites
            top_favorites = self.get_top_two_favorites(rides)
            
            race_info = {
                'race_name': race_name,
                'course_name': course_name,
                'race_time': race_time,
                'race_date': race_date,
                'distance': distance,
                'total_entries': total_entries,
                'runners': runners,
                'non_runners': non_runners,
                'top_favorites': top_favorites
            }
            
            processed_races.append(race_info)
        
        return processed_races
    
    def generate_summary_text(self, races_data):
        """Generate a human-readable summary of the race data."""
        if not races_data:
            return "No race data available."
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        summary = [
            f"Horse Racing Summary - {timestamp}",
            "=" * 50,
            ""
        ]
        
        for race in races_data:
            race_header = f"{race['race_name']}"
            summary.append(race_header)
            summary.append("-" * len(race_header))
            summary.append(f"Course: {race['course_name']}")
            summary.append(f"Time: {race['race_time']} on {race['race_date']}")
            summary.append(f"Distance: {race['distance']}")
            summary.append(f"Runners: {race['runners']} (Total entries: {race['total_entries']})")
            
            if race['non_runners'] > 0:
                summary.append(f"Non-runners: {race['non_runners']}")
            
            summary.append("Top 2 Favorites:")
            for favorite in race['top_favorites']:
                summary.append(f"  {favorite}")
            
            summary.append("")  # Empty line between races
        
        return "\n".join(summary)
    
    def save_summary(self, summary_text):
        """Save the summary to a text file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"race_summary_{timestamp}.txt"
        
        # Create output directory if it doesn't exist
        if self.output_dir and not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(summary_text)
            print(f"Summary saved to: {filepath}")
            return filepath
        except Exception as e:
            print(f"Error saving summary: {e}")
            return None
    
    def run_once(self):
        """Fetch data and generate summary once."""
        print(f"Fetching race data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Fetch data
        raw_data = self.fetch_race_data()
        if not raw_data:
            print("Failed to fetch race data.")
            return False
        
        # Process data
        races_data = self.process_race_data(raw_data)
        if not races_data:
            print("No races found in the data.")
            return False
        
        # Generate and save summary
        summary = self.generate_summary_text(races_data)
        print("\n" + summary)
        
        saved_file = self.save_summary(summary)
        
        if saved_file:
            print(f"\nProcessed {len(races_data)} races")
            return True
        else:
            return False
    
    def run_continuously(self):
        """Run the fetcher interactively with user prompts."""
        output_path = os.path.abspath(self.output_dir) if self.output_dir else "current directory"
        print(f"Output directory: {output_path}")
        print("=" * 60)
        
        try:
            while True:
                success = self.run_once()
                
                if success:
                    print("\n" + "="*50)
                    user_input = input("\nPress 'G' + Enter to fetch again, or just Enter to quit: ").strip().upper()
                    
                    if user_input == 'G':
                        print("\nFetching again...\n")
                        continue
                    else:
                        print("Goodbye!")
                        break
                else:
                    print("\n" + "="*50)
                    user_input = input("\nFetch failed. Press 'G' + Enter to try again, or just Enter to quit: ").strip().upper()
                    
                    if user_input == 'G':
                        print("\nTrying again...\n")
                        continue
                    else:
                        print("Goodbye!")
                        break
                        
        except KeyboardInterrupt:
            print("\n\nStopping fetcher...")
        except Exception as e:
            print(f"Unexpected error: {e}")

def main():
    """Main function to run the script."""
    # Configuration
    API_URL = "https://www.sportinglife.com/api/horse-racing/race?limit=10&sort_direction=ASC&sort_field=RACE_TIME"
    FETCH_INTERVAL = 300  # Not used anymore, but keeping for potential future use
    OUTPUT_DIR = "" # Set to e.g., "race_summaries" to save files in a subfolder
    
    print("Horse Racing Data Fetcher v2.2")
    print("Configured with enhanced headers to bypass bot detection.")
    print("-" * 60)
    
    # Create fetcher instance
    fetcher = HorseRacingFetcher(API_URL, FETCH_INTERVAL, OUTPUT_DIR)
    
    # Check for command line arguments
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Run once and exit (legacy mode)
        success = fetcher.run_once()
        sys.exit(0 if success else 1)
    else:
        # Run interactively
        fetcher.run_continuously()

if __name__ == "__main__":
    main()
