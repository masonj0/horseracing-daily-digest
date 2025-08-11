# Temporary script to test Selenium interaction with USTA website

import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add the current directory to the path to allow importing the main script
import sys
sys.path.append('.')

# This will import from ultimate_utopian_tipsheet_builder.py
from ultimate_utopian_tipsheet_builder import robust_fetch

def main():
    url = "http://racing.ustrotting.com/"
    print(f"--- Testing Selenium interaction with: {url} ---")

    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    # Some sites require a specific window size to render correctly
    options.add_argument('--window-size=1920,1080')
    driver = uc.Chrome(options=options)

    try:
        print("-> Loading homepage...")
        driver.get(url)

        # Wait for the day links to be present. Looking for the 'Mon' link.
        wait = WebDriverWait(driver, 20)
        day_link_xpath = "//a[contains(@id, 'lnkbtnDay4')]"
        wait.until(EC.element_to_be_clickable((By.XPATH, day_link_xpath)))
        print("-> Homepage loaded. Found Monday link.")

        # Find the link for Monday and click it
        monday_link = driver.find_element(By.XPATH, day_link_xpath)
        print("-> Clicking Monday link...")
        monday_link.click()

        # Wait for a known element on the results page to appear.
        # A good indicator is the table that holds the race cards.
        print("-> Waiting for page to update after click...")
        wait.until(EC.presence_of_element_located((By.ID, "ctl00_ctl00_cphContentArea_cphContentArea_upEntries")))
        print("-> Page updated successfully.")

        # Get the page source after the click
        print("\n--- BEGIN UPDATED HTML CONTENT ---")
        print(driver.page_source)
        print("--- END UPDATED HTML CONTENT ---")

    except Exception as e:
        print(f"\n--- INTERACTION FAILED ---")
        print(f"An error occurred: {e}")
        # Also print page source on failure for debugging
        print("\n--- PAGE SOURCE ON FAILURE ---")
        # I'll save it to a file to avoid flooding the console
        with open("debug_usta_failure.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("Saved page source to debug_usta_failure.html")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
