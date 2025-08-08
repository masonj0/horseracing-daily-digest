#!/usr/bin/env python3
"""
AtTheRaces Live Odds Scraper (v2.0)

This script scrapes the AtTheRaces AJAX endpoint that powers their
"Market Movers" tabs. This endpoint provides a clean, aggregated list of all
races and all runners for a given region and day, including live odds.

This is the recommended approach as it provides all necessary data in a
single, reliable request.
"""
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

def convert_odds_to_float(odds_str: str) -> float:
    """
    Converts a fractional odds string (e.g., '5/2', 'EVS') to a float.
    Returns a high number for invalid odds to ensure they are sorted last.
    """
    if not isinstance(odds_str, str):
        return 9999.0
    
    odds_str = odds_str.strip().upper()
    
    if 'SP' in odds_str:
        return 9999.0 # Treat 'SP' (Starting Price) as invalid for pre-race analysis
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

def fetch_and_parse_races(region: str = 'usa'):
    """
    Fetches and parses all races for a given region from the ATR AJAX endpoint.
    """
    today_str = datetime.now().strftime('%Y%m%d')
    # This is the key URL that returns a clean HTML table of all races.
    url = f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{today_str}"
    
    print(f"Fetching data from: {url}\n")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: Could not fetch the page. {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    
    all_races = []
    # Each race is in a table preceded by a `caption` tag.
    race_captions = soup.find_all('caption', string=re.compile(r"^\d{2}:\d{2}"))

    for caption in race_captions:
        race_name_full = caption.get_text(strip=True)
        race_time_match = re.match(r"(\d{2}:\d{2})", race_name_full)
        race_time = race_time_match.group(1) if race_time_match else "N/A"
        
        # The course name is in a previous 'h2' tag, so we find the parent
        # container and then find the 'h2' within it.
        course_header = caption.find_parent('div', class_='panel').find('h2')
        course_name = course_header.get_text(strip=True) if course_header else "Unknown Course"

        race_table = caption.find_next_sibling('table')
        if not race_table:
            continue

        horses = []
        for row in race_table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            if not cells:
                continue

            horse_name = cells[0].get_text(strip=True)
            # The odds are in the second cell.
            current_odds_str = cells[1].get_text(strip=True)
            
            horses.append({
                'name': horse_name,
                'odds_str': current_odds_str,
                'odds_float': convert_odds_to_float(current_odds_str)
            })

        if not horses:
            continue
            
        # Sort horses by odds to find the favorites
        horses.sort(key=lambda x: x['odds_float'])
        
        all_races.append({
            'course': course_name,
            'time': race_time,
            'field_size': len(horses),
            'favorite': horses[0] if len(horses) > 0 else None,
            'second_favorite': horses[1] if len(horses) > 1 else None,
        })
        
    return all_races

def main():
    """Main function to run the scraper and check for target races."""
    print("=" * 80)
    print("AtTheRaces.com Live Odds Analyzer")
    print("=" * 80)

    races = fetch_and_parse_races(region='usa')

    if not races:
        print("Could not retrieve any race data.")
        return

    print(f"Found {len(races)} total races. Now filtering for your criteria...\n")
    
    target_races_found = 0
    for race in races:
        # Apply all of your specific criteria
        field_size_ok = race['field_size'] < 7
        
        fav = race.get('favorite')
        fav_odds_ok = fav and fav['odds_float'] >= 1.0
        
        sec_fav = race.get('second_favorite')
        sec_fav_odds_ok = sec_fav and sec_fav['odds_float'] >= 2.5

        if field_size_ok and fav_odds_ok and sec_fav_odds_ok:
            target_races_found += 1
            print(f"--- ✅ MATCH FOUND: {race['course']} at {race['time']} ---")
            print(f"Field Size: {race['field_size']} (Condition: < 7)")
            print(f"Favorite:   {fav['name']} ({fav['odds_str']}) -> {fav['odds_float']:.2f} (Condition: >= 1.0)")
            print(f"2nd Fav:    {sec_fav['name']} ({sec_fav['odds_str']}) -> {sec_fav['odds_float']:.2f} (Condition: >= 2.5)")
            print("-" * 50 + "\n")

    if target_races_found == 0:
        print("No races matching your specific criteria were found at this time.")

if __name__ == "__main__":
    main()