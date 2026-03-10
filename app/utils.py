import re
import asyncio
import json
import random
from urllib.parse import urlencode
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from httpx import AsyncClient
from typing import Optional, Tuple, List, Dict

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.enums import SheetStatus

from dotenv import load_dotenv
import os
import csv
import pandas as pd

load_dotenv()

# Initialize once at module level
semaphore = asyncio.Semaphore(value=3)

# Load request headers JSON file
HEADERS_FILE = Path(__file__).parent / "request_headers.json"
with HEADERS_FILE.open(encoding="utf-8") as f:
    REQUEST_HEADERS = json.load(f)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

def get_google_spreadsheet_id_and_gid(google_sheet_link: str) -> Optional[Tuple[str, str]]:
    # Confirm if this requires error handling???
    pattern = r"/d/([a-zA-Z0-9-_]+).*?(?:\?|#|&)gid=(\d+)"
    match = re.search(pattern, google_sheet_link)

    if match:
        sheet_id = match.group(1)
        gid = match.group(2)
        return sheet_id, gid

    return None, None

async def async_get_sheet_status(spreadsheet_id: str, gid: str) -> SheetStatus:
    """
    Async wrapper for get_sheet_status
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        get_sheet_status,
        spreadsheet_id,
        gid
    )

def get_sheet_status(spreadsheet_id: str, gid: str) -> SheetStatus:
    """
    Check if a Google Sheet exists and is publicly accessible.

    Args:
        spreadsheet_id: The spreadsheet ID from the URL
        gid: SHeet ID (tab within the spreadsheet)

    Returns:
        Sheet status
    """
    try:
        service = build("sheets", "v4", developerKey=os.environ["GOOGLE_DEVELOPER_API_KEY"])

        sheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields='properties.title,sheets(properties(sheetId,title))'
        ).execute()

        sheets = sheet.get("sheets", [])
        gid_int = int(gid)
        sheet_exists = any(
            s.get("properties", {}).get("sheetId") == gid_int
            for s in sheets
        )

        if sheet_exists:
            return SheetStatus.PUBLIC
        
        return SheetStatus.TAB_DOES_NOT_EXIST

    except HttpError as e:
        if e.resp.status == 404:
            return SheetStatus.SHEET_DOES_NOT_EXIST

        if e.resp.status == 403:
            return SheetStatus.PRIVATE

    except Exception as e:
        print(f'Unexpected error: {str(e)}')

    return SheetStatus.SHEET_DOES_NOT_EXIST

async def get_sheet_details(google_sheet_link: str) -> Tuple[str, str, str]:
    """
    Premilinary check on the Google sheet. Get spreadsheet ID and GID if all checks passed.
    
    :param google_sheet_link: Google sheet link
    :type google_sheet_link: str
    :return: Spreadsheet ID, GID, error message
    :rtype: Tuple[str, str, str]
    """
    spreadsheet_id = gid = error_message = ""

    spreadsheet_id, gid = get_google_spreadsheet_id_and_gid(
        google_sheet_link=google_sheet_link
    )

    if not spreadsheet_id or not gid:
        error_message = "Please check the Google sheet link format."
        return "", "", error_message 

    status = await async_get_sheet_status(
        spreadsheet_id=spreadsheet_id,
        gid=gid
    )

    if status == SheetStatus.SHEET_DOES_NOT_EXIST:
        error_message = "Google sheet not found."

    if status == SheetStatus.TAB_DOES_NOT_EXIST:
        error_message = "Tab not found. Please check \"GID\" passed in the Google sheet link."

    if status == SheetStatus.PRIVATE:
        error_message = "Please provide a public Google sheet link."

    return spreadsheet_id, gid, error_message

async def async_fetch_asin_codes(spreadsheet_id: str, gid: str) -> List[str]:
    """
    Async wrapper from fetch_asin_codes
    """

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        fetch_asin_codes,
        spreadsheet_id,
        gid
    )

def fetch_asin_codes(spreadsheet_id: str, gid: str) -> List[str]:
    """
    Fetch all the ASIN codes found from the Google sheet.

    Args:
        spreadsheet_id: Google spreadsheet ID
        gid: GID for different Google spreadsheet tabs

    Returns:
        List of all ASIN codes retrieved from Google Sheet
    """

    try:
        service = build("sheets", "v4", developerKey=os.environ["GOOGLE_DEVELOPER_API_KEY"])

        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))"
        ).execute()

        sheet_name = None
        gid_int = int(gid)

        for sheet in sheet_metadata.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") == gid_int:
                sheet_name = sheet.get("properties", {}).get("title")
                break
        
        # Sheet name should exist as preliminary check was already completed
        range_name = f"'{sheet_name}'!A2:A"

        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueRenderOption="FORMATTED_VALUE"
        ).execute()

        values = result.get("values", [])

        asin_codes = [row[0] if row else "" for row in values]

        return asin_codes

    except HttpError as e:
        print(f"HTTP error: {e.resp.status} - {str(e)}")
        return []

    except Exception as e:
        print(f"Error retrieving values: {str(e)}")
        return []

def create_batches(items: List[str], batch_size: int) -> List[List[str]]:
    """
    Split a list into batches of fixed maximum size

    Args:
        items: List of items be batched
        batch_size: Maximum number of items per batch (must be > 0)

    Returns:
        Batches and their count
    
    Raises:
        ValueError: If batch size is less than 1
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    
    batches = [
        items[i: i + batch_size]
        for i in range(0, len(items), batch_size)
    ]

    return batches

def get_random_request_headers() -> Dict[str, str]:
    """Fetch a random request headers from the pre-defined list"""
    total_request_headers = len(REQUEST_HEADERS)
    idx = random.randint(0, total_request_headers - 1)

    return REQUEST_HEADERS[idx]

async def fetch_asin_code_html_response(
    client: AsyncClient,
    asin_code: str,
    amazon_marketplace: str
) -> Tuple[str, Optional[str]]:
    """
    Fetch HTML response from product's Amazon page using the product ASIN code

    Args:
        client: HTTPX session client
        asin_code: ASIN code of Amazon product
        amazon_marketplace: Amazon's marketplace URL. For example: amazon.com, amazon.co.uk, amazon.in etc.

    Returns:
        ASIN code and it's HTML response or None
    """

    AMAZON_URL = f"https://{amazon_marketplace}/dp/{asin_code}"
    SCRAPPING_API_TOKEN = os.environ["SCRAPPING_API_TOKEN"]
    params = {
        "url": AMAZON_URL,
        "token": SCRAPPING_API_TOKEN
    }
    target_url = f"http://api.scrape.do/?{urlencode(params)}"

    async with semaphore:
        try:
            response = await client.get(target_url, timeout=20)
            response.raise_for_status()
            return asin_code, response.text
        except Exception as e:
            print(f"Error fetching {asin_code}: {e}")
            return asin_code, None

async def process_batch(client: AsyncClient, batch: List[str], amazon_marketplace: str) -> List[Tuple[str, Optional[str]]]:
    """
    Fetch HTML content for each batch

    Args:
        client: HTTPX client session
        batch: List of ASIN codes
        amazon_marketplace: Amazon's marketplace URL. For example: amazon.com, amazon.co.uk, amazon.in etc.

    Returns:
        ASIN codes and their HTML response from the product's Amazon page or None
    """
    tasks = [fetch_asin_code_html_response(client, asin_code, amazon_marketplace) for asin_code in batch]
    results = await asyncio.gather(*tasks)

    data = []

    for asin_code, html_response in results:
        data.append(( asin_code, html_response ))
    
    return data

def clean_amazon_spec(text: str) -> Optional[str]:
    """Remove all invisible Unicode characters from text"""
    if not text:
        return None
    return re.sub(r'[\u200e\u200f\u2060\u00a0]', '', text or '').strip(' ;.')

def retrieve_data_from_html_response(asin_code: str, html_response: str | None) -> Dict[str, Optional[str]]:
    """
    Returns all the product relevant data like product title, descrition, pricing, weight, dimenesions etc. from the HTML response

    Args:
        asin_code: ASIN code of the Amazon product
        html_response: HTML response recevied from the Amazon page. In case of not found pages, the value will be none

    Returns:
        A dictionary containing all the product data
    """

    if html_response == None:
        return {
            "ASIN": asin_code,
            "Title": "-",
            "Description": "-",
            "Image": "-",
            "Sale price": "-",
            "Brand": "-",
            "Dimensions": "-",
            "Weight": "-"
        }

    soup = BeautifulSoup(html_response, "html.parser")
    product_data = {
        "ASIN": asin_code
    }

    # Primary product data elements
    product_title_el = soup.find("span", id="productTitle")
    product_description_el = soup.find("div", id="featurebullets_feature_div")

    # Image elements
    product_main_img_el = soup.select_one("div#imgTagWrapperId img")

    # Pricing elements
    # product_mrp_el = soup.select_one("div#corePriceDisplay_desktop_feature_div span.apex-basisprice-value span.a-offscreen")
    product_sale_price_el = soup.select_one("div#corePriceDisplay_desktop_feature_div span.apex-pricetopay-value")

    # Product overview and technical detail table elements
    product_overview_table_row_els = soup.select("div#productOverview_feature_div table tbody tr")
    product_technical_detail_table_row_els = soup.select("table#productDetails_techSpec_section_1 tr")
    product_overview_and_technical_detail_table_row_els = [*product_overview_table_row_els, *product_technical_detail_table_row_els]

    # Product overview and technical detail values
    product_brand_name_value = product_weight_value = product_dimensions_value = None

    # Retrieve data from selected elements
    if product_title_el:
        product_title_value = product_title_el.get_text(strip=True)
        product_data["Title"] = clean_amazon_spec(product_title_value)
    else:
        product_data["Title"] = "-"

    if product_description_el:
        product_description_value = ""
        product_description_list_items = product_description_el.select("ul li")
        for product_description_list_item in product_description_list_items:
            product_description_value += f"• {product_description_list_item.get_text(strip=True)} "
        
        product_description_value = product_description_value.strip()

        if product_description_value == "":
            product_data["Description"] = "-"
        else:
            product_data["Description"] = product_description_value
    else:
        product_data["Description"] = "-"

    if product_main_img_el:
        product_img = product_main_img_el.get("data-old-hires", "-")
        if product_img == "-":
            product_img = product_main_img_el.get("src", "-")
        product_data["Image"] = product_img
    else:
        product_data["Image"] = "-"

    # if product_mrp_el:
    #     product_mrp_value = product_mrp_el.get_text(strip=True)
    #     product_data["MRP"] = clean_amazon_spec(product_mrp_value)
    # else:
    #     product_data["MRP"] = "-"

    if product_sale_price_el:
        product_sale_price_value = product_sale_price_el.get_text(strip=True)
        product_data["Sale price"] = clean_amazon_spec(product_sale_price_value)
    else:
        product_data["Sale price"] = "-"
    
    if product_overview_and_technical_detail_table_row_els:
        for tr in product_overview_and_technical_detail_table_row_els:
            th_el = tr.select_one("th")
            td_el = tr.select_one("td")

            if th_el and td_el:
                attribute_name = th_el.get_text(strip=True).lower()
                value = td_el.get_text(strip=True)

                if "brand" in attribute_name:
                    product_brand_name_value = value
                elif "dimension" in attribute_name:
                    product_dimensions_value = value
                elif "weight" in attribute_name:
                    product_weight_value = value
    
    if product_brand_name_value:
        product_data["Brand"] = clean_amazon_spec(product_brand_name_value)
    else:
        product_data["Brand"] = "-"

    if product_dimensions_value:
        product_data["Dimensions"] = clean_amazon_spec(product_dimensions_value)
    else:
        product_data["Dimensions"] = "-"

    if product_weight_value:
        product_data["Weight"] = clean_amazon_spec(product_weight_value)
    else:
        product_data["Weight"] = "-"

    return product_data

def generate_unique_title() -> str:
    """
    Generate and return a unique title for CSV, XLSX or Google sheet

    Returns:
        Unique title
    """
    timestamp_readable = datetime.now().strftime("%d %B %Y | %H:%M:%S")
    title = f"Exported data - {timestamp_readable}"

    return title


def export_data_csv(rows=List[Dict[str, Optional[str]]]) -> str:
    """
    Write data retrieved from Amazon pages to a new CSV file

    Returns:
        Publicly accessible link for the CSV file
    """
    PUBLIC_DIR = "public"
    filename = f"{generate_unique_title()}.csv"
    filepath = os.path.join(PUBLIC_DIR, filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=rows[0].keys(),
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    
    link = f"http://localhost:8000/public/{filename}"
    return link

def export_data_excel(rows=List[Dict[str, Optional[str]]]) -> str:
    """
    Write data retrieved from Amazon pages to a new Excel sheet file

    Returns:
        Publicly accessible link for the Excel sheet file
    """
    PUBLIC_DIR = "public"
    filename = f"{generate_unique_title()}.xlsx"
    filepath = os.path.join(PUBLIC_DIR, filename)

    try:
        df = pd.DataFrame(rows)
        df.to_excel(
            filepath,
            index=False,
            engine="openpyxl"
        )

        link = f"http://localhost:8080/public/{filename}"
        return link
    except Exception as e:
        print(e)
        return ""

def export_data_google_sheets(rows=List[Dict[str, Optional[str]]]) -> str:
    """
    Write data retrieved from Amazon pages to a new Google spreadsheet

    Returns:
        Publicly accessible link for the exported Google sheet
    """
    credentials = Credentials(
        None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=SCOPES
    )

    sheets_service = build("sheets", "v4", credentials=credentials)
    drive_service = build("drive", "v3", credentials=credentials)

    spreadsheet_title = generate_unique_title()

    file_metadata = {
        "name": spreadsheet_title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": ["1c2IUudIezFzuwbGTF2CGMDORXkKhAj0K"]
    }

    file = drive_service.files().create(
        body=file_metadata,
        fields="id"
    ).execute()

    exported_spreadsheet_id = file["id"]

    # First make spreadsheet public
    drive_service.permissions().create(
        fileId=exported_spreadsheet_id,
        body={"type": "anyone", "role": "writer"}
    ).execute()

    # Update exported spreadsheet with data retrieved from Amazon.
    exported_range_name = "Sheet1!A1"
    exported_headers = [
        ("ASIN", 120),
        ("Title", 350),
        ("Description", 400),
        ("Image", 100),
        # ("MRP", 100),
        ("Sale price", 100),
        ("Brand", 120),
        ("Dimensions", 180),
        ("Weight", 150)
    ]
    exported_rows = []
    column_dimension_properties_for_batch_update = []

    for item in rows:
        exported_row = []
        for idx, header_data in enumerate(exported_headers, start=0):
            header_name, column_width = header_data
            column_dimension_properties_for_batch_update.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": 0,
                        "dimension": "COLUMNS",
                        "startIndex": idx,
                        "endIndex": idx + 1
                    },
                    "properties": {"pixelSize": column_width},
                    "fields": "pixelSize"
                }
            })
            if header_name == "Image":
                image_url = item.get(header_name, "-")
                if image_url == "-":
                    exported_row.append(image_url)
                else:
                    exported_row.append(f'=IMAGE("{image_url}")')
            else:
                exported_row.append(item[header_name])
        exported_rows.append(exported_row)

    header_row = [header_name for header_name, _ in exported_headers]
    exported_spreadsheet_rows = [header_row] + exported_rows

    body = {
        "values": exported_spreadsheet_rows,
    }

    sheets_service.spreadsheets().values().update(
        spreadsheetId=exported_spreadsheet_id,
        range=exported_range_name,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

    SPREADSHEET_ROW_HEIGHT = 54

    batch_update_requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": 0,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": len(exported_spreadsheet_rows),
                    },
                    "properties": {
                        "pixelSize": SPREADSHEET_ROW_HEIGHT
                    },
                    "fields": "pixelSize"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": 0,
                        "startRowIndex": 0,
                        "endRowIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True
                            }
                        }
                    },
                    "fields": "userEnteredFormat.textFormat.bold"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": 0,
                        "startRowIndex": 0,
                        "endRowIndex": len(exported_spreadsheet_rows)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "LEFT",
                            "verticalAlignment": "TOP",
                            "wrapStrategy": "WRAP"
                        }
                    },
                    "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy"
                }
            },
            {
                "updateSpreadsheetProperties": {
                    "properties": {
                        "importFunctionsExternalUrlAccessAllowed": True
                    },
                    "fields": "importFunctionsExternalUrlAccessAllowed"
                }
            },
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": 0,
                        "dimension": "ROWS",
                        "startIndex": len(exported_spreadsheet_rows),
                        "endIndex": 2_000_000
                    }
                }
            },
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": 0,
                        "dimension": "COLUMNS",
                        "startIndex": len(header_row),
                        "endIndex": 1_000
                    }
                }
            }
    ]

    batch_update_requests.extend(column_dimension_properties_for_batch_update)

    batch_update_request_body = {
        "requests": batch_update_requests
    }

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=exported_spreadsheet_id,
        body=batch_update_request_body
    ).execute()

    link = f"https://docs.google.com/spreadsheets/d/{exported_spreadsheet_id}"

    return link