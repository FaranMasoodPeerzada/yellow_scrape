import asyncio
import aiohttp
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import csv
import random
import os

async def exponential_backoff():
    for i in range(5):  # Retry up to 5 times
        delay = 2**i + random.uniform(0, 1)  # Exponential backoff with jitter
        await asyncio.sleep(delay)

def create_new_sheet(client, folder_id, base_sheet_name, sheet_number):
    base_split = base_sheet_name.replace('k','').split('-')
    new_sheet_name = str(int(base_split[1]) * (sheet_number)) + str('k-') + str(int(base_split[1]) * (sheet_number+1)) + str('k')
    file_metadata = {
        'name': new_sheet_name,
        'parents': [folder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet'
    }
    print(f"Trying to create new sheet ({base_sheet_name}_{sheet_number}) in {base_sheet_name} Folder")
    try:
        sheet_detail = client.create(new_sheet_name, folder_id=folder_id)
        sheet_name = sheet_detail.title
        sheet_id = sheet_detail.id
        print(f"New sheet created ({sheet_name}) in {base_sheet_name} Folder : sheet_id : {sheet_id}")
        return client.open(sheet_name, folder_id=folder_id).sheet1
    except Exception as e:
        print(f"Error creating new sheet: {e}")
        return None

locations = {
    'Los Angeles': ['Huntington Park', 'Glendale', 'West Hollywood', 'Alhambra', 'San Gabriel'],
    'San Diego': ['Carmel Mtn', 'Carmel Mountain Ranch', 'Coronado Island', 'Coronado', 'National City'],
    'San Jose': ['Santa Clara', 'Campbell', 'Milpitas', 'Alviso', 'Cupertino'],
    'San Francisco': ['Daly City', 'Brisbane', 'Sausalito', 'Belvedere Tiburon', 'Emeryville'],
    'Fresno': ['Calwa', 'Clovis'],
    'Sacramento': ['West Sacramento', 'Mcclellan', 'Rio Linda', 'Elverta', 'Carmichael'],
    'Long Beach': ['Signal Hill', 'Wilmington', 'Seal Beach', 'San Pedro', 'Lakewood'],
    'Oakland': ['Emeryville', 'Piedmont', 'Alameda', 'Berkeley', 'Albany'],
    'Bakersfield': ['Oildale', 'Edison'],
    'Anaheim': ['Fullerton', 'Garden Grove', 'Orange', 'Buena Park', 'Brea', 'Santa Ana']
}

search_queries = ['Marketing Agency']

url_format = 'https://www.yellowpages.com/search?search_terms={}&geo_location_terms={}'

# List to store complete URLs
url_list = []

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/faran/OneDrive/Desktop/YP/pythonProject/creds/service_account.json', scope)
client = gspread.authorize(credentials)

folder_id1 = '1VP4cK4qXaVexJecVG79TAKWbDRKmeeu7'
folder_id2 = '10sZI2kj5ymaATtlPHV_h1Fxglvl7205b'

for location, sublocations in locations.items():
    for sublocation in sublocations:
        for query in search_queries:
            search_url = url_format.format(query.replace(' ', '+'), sublocation.replace(' ', '+'))
            url_list.append(search_url)

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def get_listing_details(session, listing_url):
    html = await fetch(session, listing_url)
    soup = BeautifulSoup(html, 'html.parser')

    email = soup.find('a', href=lambda href: href and 'mailto:' in href)
    phone = soup.find(class_='phone')

    email = email['href'].replace('mailto:', '') if email else None
    phone = phone.get_text().strip() if phone else None

    return email, phone

async def main():
    base_url = 'https://www.yellowpages.com'
    page = 1
    total_with_email = 0
    url_counter = 0

    start_time = time.time()

    with open('Marketing_details.csv', 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Title", "URL", "Business Phone", "Email"])

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'C:/Users/faran/OneDrive/Desktop/YP/pythonProject/creds/service_account.json', scope)
        client = gspread.authorize(credentials)

        folder_id1 = '1VP4cK4qXaVexJecVG79TAKWbDRKmeeu7'
        folder_id2 = '10sZI2kj5ymaATtlPHV_h1Fxglvl7205b'

        base_sheet_name1 = '0-25k'
        base_sheet_name2 = '0-100k'

        current_sheet1 = client.open(base_sheet_name1, folder_id=folder_id1).sheet1
        current_sheet2 = client.open(base_sheet_name2, folder_id=folder_id2).sheet1

        current_row_count1 = 0
        current_row_count2 = 0

        max_records_per_sheet1 = 25000
        max_records_per_sheet2 = 100000

        max_buffer_length = 5

        sheet_number1 = 0
        sheet_number2 = 0

        buffer1 = []
        buffer2 = []

        checkpoint_file = 'checkpoint.txt'

        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                last_processed_url_index = int(f.readline())
        else:
            last_processed_url_index = 0

        async with aiohttp.ClientSession() as session:
            for index, search_url in enumerate(url_list[last_processed_url_index:], start=last_processed_url_index):
                page = 1
                url_counter += 1
                while True:
                    html = await fetch(session, search_url)
                    soup = BeautifulSoup(html, 'html.parser')

                    listings = soup.find_all('div', class_='result')

                    tasks = []
                    for listing in listings:
                        business_name = listing.find('a', class_='business-name').get_text().strip()
                        listing_url = base_url + listing.find('a', class_='business-name')['href']

                        task = asyncio.create_task(get_listing_details(session, listing_url))
                        tasks.append((business_name, listing_url, task))

                    for business_name, listing_url, task in tasks:
                        email, phone = await task

                        if email and phone:
                            total_with_email += 1
                            writer.writerow([business_name, listing_url, phone, email])
                            buffer1.append([business_name, listing_url, phone, email])
                            buffer2.append([business_name, listing_url, phone, email])

                            try:
                                current_row_count1 += 1
                                current_row_count2 += 1

                                if len(buffer1) >= max_buffer_length:
                                    current_sheet1.append_rows(buffer1)
                                    buffer1 = []

                                if len(buffer2) >= max_buffer_length:
                                    current_sheet2.append_rows(buffer2)
                                    buffer2 = []

                                if current_row_count1 >= max_records_per_sheet1:
                                    sheet_number1 += 1
                                    current_sheet1 = create_new_sheet(client, folder_id1, base_sheet_name1, sheet_number1)
                                    current_row_count1 = 0

                                if current_row_count2 >= max_records_per_sheet2:
                                    sheet_number2 += 1
                                    current_sheet2 = create_new_sheet(client, folder_id2, base_sheet_name2, sheet_number2)
                                    current_row_count2 = 0

                            except Exception as e:
                                print("Error Occurs during adding data in G-Sheet : ", e)
                                await exponential_backoff()  # Apply exponential backoff before retrying

                    print("---------------Status---------------")
                    print(
                        f"Scraped {total_with_email} Emails from {page} page{'s' if page > 1 else ''} and  {url_counter} URL{'s' if url_counter > 1 else ''}")
                    print(f"Current page URL: {search_url}")

                    next_button = soup.find('a', class_='next')
                    if not next_button:
                        break

                    page += 1
                    if 'page=' in search_url:
                        search_url = search_url[:search_url.rfind('page=')] + f'page={page}'
                    else:
                        search_url = f'{search_url}&page={page}'

                    # Save progress to checkpoint file
                    with open(checkpoint_file, 'w') as f:
                        f.write(str(index))

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {total_time} seconds")

if __name__ == "__main__":
    print(f"Total URLs based on different locations:  {len(url_list)}")
    asyncio.run(main())
