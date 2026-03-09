import os
import json
import traceback
import asyncio
import httpx
from typing import Annotated

from fastapi import FastAPI, Form, File, UploadFile, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.utils import get_sheet_details, async_fetch_asin_codes,\
    create_batches, process_batch, retrieve_data_from_html_response,\
    export_data_csv, export_data_excel, export_data_google_sheets
import pandas as pd

from fastapi.middleware.cors import CORSMiddleware        

app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PUBLIC_DIR = "public"
os.makedirs(PUBLIC_DIR, exist_ok=True)
app.mount("/public", StaticFiles(directory=PUBLIC_DIR), name="public")

@app.get("/")
async def main():
    return JSONResponse({
        "message": "Amazon product web scrapper using LLM"
    }, status_code=200) 

@app.post("/retrieve-asin-codes")
async def retrieve_asin_codes(
    data_source_type: Annotated[str, Form(...)],
    google_sheet_link: Annotated[str | None, Form()] = None,
    file: Annotated[UploadFile | None, File()] = None
):

    if data_source_type not in {"google_sheet_link", "local_file"}:
        raise HTTPException(422, detail="data_source_type must be 'google_sheet_link' or 'local_file'")

    ALLOWED_EXTENSIONS = [".csv", ".xlsx"]

    asin_codes = []

    if data_source_type == "google_sheet_link":
        if not google_sheet_link or not isinstance(google_sheet_link, str):
            raise HTTPException(422, detail="For google_sheet_link, data_source_value must be a valid URL string")
        source_url = google_sheet_link.strip()
        if not source_url.startswith(("https://docs.google.com/spreadsheets", "https://sheets.google.com")):
            raise HTTPException(422, detail="Invalid Google Sheet URL format")

        spreadsheet_id, gid, error_message = await get_sheet_details(google_sheet_link=google_sheet_link)

        if error_message:
            raise HTTPException(400, detail=error_message)

        asin_codes = await async_fetch_asin_codes(
            spreadsheet_id=spreadsheet_id,
            gid=gid
        )

    elif data_source_type == "local_file":
        if not file or not file.filename:
            raise HTTPException(422, detail="For local_file, you must upload a file")

        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(422, detail=f"Only {', '.join(ALLOWED_EXTENSIONS)} files allowed")

        df = None

        try:
            if ext == ".csv":
                df = pd.read_csv(file.file, header=0, dtype=str, keep_default_na=False)
            else:
                df = pd.read_excel(file.file, header=0, dtype=str, encoding="utf-8", encoding_errors="ignore", engine="openpyxl")
        except Exception as e:
            print(e)
            raise HTTPException(422, detail="Unable to read the file. Please try a different file or a different source")
        
        if df.empty or len(df.columns) == 0:
            raise HTTPException(422, detail="Uploaded file is empty or has no columns")

        asin_column = df.iloc[:, 0]
        asin_column = asin_column.iloc[1:]

        for val in asin_column:
            if pd.isna(val):
                continue
            cleaned = str(val).strip()
            if cleaned:
                asin_codes.append(cleaned)

    reduced = len(asin_codes) > 50
    asin_codes = asin_codes[:50]

    return JSONResponse({
        "reduced": reduced,
        "asin_codes": asin_codes
    }, status_code=200)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            try:
                json_payload = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=30.0
                )

                if not isinstance(json_payload, dict):
                    await websocket.send_json({
                        "type": "error",
                        "message": "Invalid payload format."
                    })
                    break

                asin_codes = json_payload.get("asin_codes", None)

                supported_export_type = ["csv", "xlsx", "google-sheet"]
                export_type = json_payload.get("export_type", "csv")
                if export_type not in supported_export_type:
                    export_type = "csv"

                amazon_marketplace = json_payload.get("amazon_marketplace", "amazon.com")
                supported_amazon_marketplaces = [
                    "amazon.com",
                    "amazon.co.uk",
                    "amazon.de",
                    "amazon.fr",
                    "amazon.it",
                    "amazon.es",
                    "amazon.ca",
                    "amazon.co.jp",
                    "amazon.com.au",
                    "amazon.in",
                    "amazon.com.mx",
                    "amazon.nl"
                ]
                if amazon_marketplace not in supported_amazon_marketplaces:
                    pass

                if not asin_codes or not isinstance(asin_codes, list) or len(asin_codes) == 0:
                    await websocket.send_json({
                        "type": "error",
                        "message": "No ASIN codes were provided."
                    })
                    break

                # For live progress UI in client
                total_steps = int(max(1, len(asin_codes) / 10))
                progress = 0
                progress_step_value = 68 / total_steps
                steps = [8, 22]
                for i in range(0, total_steps):
                    prev = steps[-1]
                    steps.append(min(95, prev + progress_step_value))
                steps.append(100)
                steps.append(100)
                steps.reverse()

                next_progress_stop = steps.pop()
                await websocket.send_json({
                    "current": "reading-asin-list",
                    "finished": [],
                    "progress": progress,
                    "next_progress_stop": next_progress_stop
                })
                await asyncio.sleep(2)

                progress = next_progress_stop
                next_progress_stop = steps.pop()

                await websocket.send_json({
                    "current": "connecting-to-amazon",
                    "finished": ["reading-asin-list"],
                    "progress": progress,
                    "next_progress_stop": next_progress_stop
                })
                await asyncio.sleep(4)

                batches = create_batches(
                    items=asin_codes,
                    batch_size=10
                )
                total_batches = len(batches)

                all_html_responses = []
                rows = []

                async with httpx.AsyncClient(follow_redirects=True) as client:
                    for i in range(0, total_batches):
                        current_batch = batches[i]
                        current_batch_html_responses = await process_batch(client, current_batch, amazon_marketplace)
                        all_html_responses.extend(current_batch_html_responses)

                        progress = next_progress_stop
                        next_progress_stop = steps.pop()
                        await websocket.send_json({
                            "current": "fetching-product-data",
                            "finished": ["reading-asin-list", "connecting-to-amazon"],
                            "progress": progress,
                            "next_progress_stop": next_progress_stop
                        })

                    progress = next_progress_stop
                    next_progress_stop = steps.pop()
                    await websocket.send_json({
                        "current": "processing-and-enriching",
                        "finished": ["reading-asin-list", "connecting-to-amazon", "fetching-product-data"],
                        "progress": progress,
                        "next_progress_stop": next_progress_stop
                    })

                for response in all_html_responses:
                    asin_code, html_content = response
                    product_data = retrieve_data_from_html_response(asin_code, html_content)
                    rows.append(product_data)

                progress = next_progress_stop
                next_progress_stop = steps.pop()
                await websocket.send_json({
                    "current": "generating-export-file",
                    "finished": ["reading-asin-list", "connecting-to-amazon", "fetching-product-data", "processing-and-enriching"],
                    "progress": progress,
                    "next_progress_stop": next_progress_stop
                })
                await asyncio.sleep(3)

                if export_type == "csv":
                    exported_link = export_data_csv(rows=rows)
                elif export_type == "xlsx":
                    exported_link = export_data_excel(rows=rows)
                else:
                    exported_link = export_data_google_sheets(rows=rows)

                await websocket.send_json({
                    "current": "generating-export-file",
                    "finished": ["reading-asin-list", "connecting-to-amazon", "fetching-product-data", "processing-and-enriching", "generating-export-file"],
                    "exported_link": exported_link,
                    "progress": progress,
                    "next_progress_stop": next_progress_stop
                })

                break

            except asyncio.TimeoutError:
                await websocket.send_json({
                    "type": "ping"
                })
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON."
                })
                break
            except WebSocketDisconnect:
                print("Client disconnected normally")
                break
    except Exception as e:
        print(f"WebSocket error: {e}")
        traceback.print_exc()
    finally:
        try:
            print("Closing websocket connection")
            await websocket.close()
        except:
            pass