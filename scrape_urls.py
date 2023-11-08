import random
import subprocess
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
import time
import csv
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium.common.exceptions import NoSuchElementException

# Set up the connection to the Google Sheet
# Use the JSON file you downloaded to use the credentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(
    '/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/credentials.json', scope)
client = gspread.authorize(creds)

# Open the Google Spreadsheet
sheet = client.open_by_key("1kK9oboTzd8sBuNsMCxJIq5HHYmXSOnUI9NYPJh98-CU").sheet1


def log(msg):
    # Log the message with current timestamp.
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}")

    # Also write it to local file scrape_urls.log
    with open('/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/scrape_urls.log', 'a') as file:
        file.write(time.strftime('%Y-%m-%d %H:%M:%S') + ' ' + msg + '\n')


def check_if_running():
    # Checks to see if this process is already running using subprocess.
    # Get the list of processes
    processes = subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE)
    # Get the output of the processes
    out, err = processes.communicate()
    # Check to see if this process is in the list of processes
    process_count = 0
    for line in out.splitlines():
        if b"scrape_urls.py" in line and b"cron-urls.log" not in line:
            print(line)
            process_count = process_count + 1
            if process_count == 2:
                return True
    return False


def append_rows_to_sheet(batch_data):
    """Append a batch of rows to the Google sheet."""
    if batch_data:
        sheet.append_rows(batch_data)
        log(f"Batch of {len(batch_data)} rows added to the spreadsheet.")
    else:
        log("No data to add to the spreadsheet.")


def scrape_urls():
    log("Scrape URLs function started.")

    log("Initializing web driver for Selenium.")

    # Pick a random proxy from proxy_list.txt file
    proxy = random.choice(
        list(open('/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/proxy_list.txt'))).strip()
    log(f"Selected proxy: {proxy}")

    # The proxy is in the format of IP:POST:USERNAME:PASSWORD
    # Split it into its components
    proxy_ip = proxy.split(':')[0]
    proxy_port = proxy.split(':')[1]
    proxy_username = proxy.split(':')[2]
    proxy_password = proxy.split(':')[3]

    # Set up proxy details
    proxy_options = {
            'proxy': {
                    'http':     f'http://{proxy_username}:{proxy_password}@{proxy_ip}:{proxy_port}',
                    'https':    f'https://{proxy_username}:{proxy_password}@{proxy_ip}:{proxy_port}',
                    'no_proxy': 'localhost',
            },
    }

    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument(
        f'user-data-dir=/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/chrome-data')  # Path to your chrome data directory

    # Initialize the Chrome driver with selenium wire options and Chrome options
    driver = webdriver.Chrome(
            seleniumwire_options=proxy_options,
            options=chrome_options
    )

    log("Web driver initialized.")

    page_num = 1
    if os.path.exists('/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/page_num.txt'):
        with open('/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/page_num.txt', 'r') as file:
            page_num = int(file.read())
            log(f"Starting from page number {page_num}.")
    else:
        log("page_num.txt not found. Starting from page 1.")

    while page_num < 14406:
        log(f"Processing page number {page_num}.")
        # time.sleep(5)

        url = f"https://myip.ms/browse/sites/{page_num}/ownerID/376714/ownerID_A/1"
        log(f"Attempting to access URL: {url}")
        driver.get(url)
        log(f"Accessed URL: {url}")

        try:
            rows = driver.find_element(By.ID, 'sites_tbl').find_element(By.TAG_NAME, 'tbody').find_elements(By.TAG_NAME,
                                                                                                            'tr')
        except NoSuchElementException:
            log("Table rows not found. Captcha likely required. Waiting...")

            # Create a macOS notification to alert the user that a captcha is required
            os.system("""
                        osascript -e 'display notification "Captcha required. Please solve the captcha and then press Enter." with title "Captcha Required"'
                    """)

            time.sleep(30)
            try:
                rows = driver.find_element(By.ID, 'sites_tbl').find_element(By.TAG_NAME, 'tbody').find_elements(
                    By.TAG_NAME, 'tr')
            except NoSuchElementException:
                log("Table rows still not found. Aborting.")
                break

        log("Table rows found.")

        # Create a batch for all new rows
        batch_data = []

        for row in rows:
            tds = row.find_elements(By.TAG_NAME, 'td')
            if len(tds) > 1:
                scraped_url = tds[1].text
                log(f"Scraped URL: {scraped_url}")

                # Check for the URL's existence in the Google Sheet
                # cell = sheet.findall(scraped_url)
                # exists = any([True for c in cell if c.row == c.row])
                # if exists:
                #     log(f"URL already exists in the spreadsheet: {scraped_url}")
                # else:
                    # If URL doesn't exist, add it to the batch data with the current date/time
                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                batch_data.append(['http://' + scraped_url, '', current_time])
                log(f"Prepared URL and date for batch addition to the spreadsheet: {scraped_url}, {current_time}")

        # Append all new rows in a single operation
        append_rows_to_sheet(batch_data)

        page_num += 1
        with open('/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/page_num.txt', 'w') as file:
            file.write(str(page_num))
            log(f"Saved current page number: {page_num}")

    driver.quit()
    log("Web driver closed. Scraping process completed.")


# Check to see if this process is already running. If so, exit the program.
if check_if_running():
    log(f"💸 Process already running. Exiting...")
    exit()

exit()
scrape_urls()
