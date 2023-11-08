import subprocess

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import selenium
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
import re

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def log(msg):
    # Log the message with current timestamp.
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    formatted_message = f"{timestamp} {msg}"
    print(formatted_message)

    # Also write it to local file scrape_emails.log
    with open('/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/scrape_emails.log', 'a') as file:
        file.write(formatted_message + '\n')


def check_if_running():
    # Checks to see if this process is already running using subprocess.
    # Get the list of processes
    processes = subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE)
    # Get the output of the processes
    out, err = processes.communicate()
    # Check to see if this process is in the list of processes
    process_count = 0
    for line in out.splitlines():
        if b"scrape_emails.py" in line and b"cron-emails.log" not in line:
            print(line)
            process_count = process_count + 1
            if process_count == 2:
                return True
    return False


def extract_email_from_text(body_text):
    # Regular expression pattern for matching an email
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    # Search the text for the pattern
    match = re.search(email_pattern, body_text)

    # If a match is found, return the email, otherwise return None
    return match.group(0) if match else None


# Function to extract email
def extract_email_from_url(url):
    try:
        log(f"Extracting email from: {url}")
        driver.get(url)

        # Try to find an email on the home page
        body_text = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "html"))).text

        email = extract_email_from_text(body_text)
        if email:
            log("Email found on the home page.")
            return email  # Return the first found email

        # If no email found, try to go to the contact page
        log("No email found on the home page.")
        try:
            contact_link = driver.find_element(By.XPATH, "//a[contains(., 'Contact') or contains(., 'contact')]")

            # navigate to the contact page by webdriver
            driver.get(contact_link.get_attribute("href"))

            log("Contact page found.")

            body_text = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "html"))).text

            email = extract_email_from_text(body_text)
            if email:
                log("Email found on the contact page.")
                return email
            else:
                log("No email found on the contact page.")
        except selenium.common.exceptions.NoSuchElementException:
            log("No contact page found.")
            return None

        return None
    except Exception as e:
        log(f"An error occurred with {url}: {e}")
        return None


# Check to see if this process is already running. If so, exit the program.
if check_if_running():
    log(f"💸 Process already running. Exiting...")
    exit()

# Use the JSON file you downloaded to use the credentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(
        '/Users/eddymoskalenko/PycharmProjects/scrape-shopify-urls/credentials.json', scope)
client = gspread.authorize(creds)

# Open the spreadsheet
sheet = client.open_by_key("1kK9oboTzd8sBuNsMCxJIq5HHYmXSOnUI9NYPJh98-CU").sheet1
log("Spreadsheet opened successfully.")

# Get all values in the first column (URLs) and second column (Emails)
urls = sheet.col_values(1)[1:]  # assuming the first row is the header
existing_emails = sheet.col_values(2)[1:]  # assuming the first row is the header
log(f"Retrieved {len(urls)} URLs to process.")

# Configure the WebDriver
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run in headless mode
driver = webdriver.Chrome(options=options)
log("WebDriver configured successfully.")


def batch_update_emails(batch_updates):
    if batch_updates:
        # Assuming 'update_cells' can take a list of tuples (row, column, value)
        sheet.update_cells(batch_updates)
        log(f"Batch updated {len(batch_updates)} cells with emails.")


# Starting with an empty list for batch updates
batch_updates = []

for i, url in enumerate(urls, start=2):  # Start at 2 to account for header in the spreadsheet

    # Retrieve the corresponding email from the second column, if any
    if (i - 2) in range(len(existing_emails)):
        log(f"Email already exists for {url}. Skipping.")
        continue

    email = extract_email_from_url(url)

    if email:
        log(f"Email for {url} found, queued for update.")
        batch_updates.append((i, 2, email))  # Append the row, column, and email for batch update
    else:
        log(f"No email found for {url}, 'Not Found' queued for update.")
        batch_updates.append((i, 2, "Not Found"))  # Mark email as "Not Found" for batch update

    # If we've reached a batch of 100, send the update
    if len(batch_updates) >= 100:
        batch_update_emails(batch_updates)
        batch_updates = []  # Reset the batch_updates for the next batch

# Don't forget to send the last batch if it's less than 100
batch_update_emails(batch_updates)

# Close the driver
driver.quit()
log("WebDriver closed and script execution finished.")
