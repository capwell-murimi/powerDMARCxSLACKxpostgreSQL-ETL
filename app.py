from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import pandas as pd
import psycopg2
import re
import os
import html
from dotenv import load_dotenv
import uvicorn
from fastapi import FastAPI
import threading


load_dotenv()

# Initialize Slack App
app = App(token=os.getenv('SLACK_BOT_TOKEN'))

# Initialize DataFrame
alerts_df = pd.DataFrame(columns=["title", "account_name", "monitoring_group", "assets_blocklisted", "zone_names"])

# Parser function to extract fields from Slack message text
def extract_fields(text):
    text = html.unescape(text)  # Decode &lt; and &gt;

    title = re.search(r'Title:\s*(.+)', text)
    account_name = re.search(r'Account Name:\s*(.+)', text)
    monitoring_group = re.search(r'Monitoring Group:\s*(.+)', text)

    # Extract full lines
    assets_line = re.search(r'Assets Blocklisted:.*', text)
    zone_names_line = re.search(r'Zone Names:.*', text)
    assets_blocklisted = re.findall(r'<http.*?\|(.*?)>', assets_line.group(0)) if assets_line else []
    zone_names = re.findall(r'<http.*?\|(.*?)>', zone_names_line.group(0)) if zone_names_line else []

    return {
        "title": title.group(1).strip() if title else None,
        "account_name": account_name.group(1).strip() if account_name else None,
        "monitoring_group": monitoring_group.group(1).strip() if monitoring_group else None,
        "assets_blocklisted": ', '.join(assets_blocklisted) if assets_blocklisted else None,
        "zone_names": ', '.join(zone_names) if zone_names else None
    }



# Slack message listener
@app.event("message")
def handle_message(event, say):
    print("📩 Received Slack message event:", event)
    text = event.get("text", "")

    if "blocklisted" in text.lower():
        data = extract_fields(text)
        data["channel"] = event.get("channel")
        data["timestamp"] = event.get("ts")

        # Append to DataFrame
        global alerts_df
        new_row = pd.DataFrame([data])
        alerts_df = pd.concat([alerts_df, new_row], ignore_index=True)

        # Insert into Supabase PostgreSQL
        try:
            conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
            cursor = conn.cursor()

            # Ensure the alerts table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    title TEXT,
                    account_name TEXT,
                    monitoring_group TEXT,
                    assets_blocklisted TEXT,
                    zone_names TEXT,
                    channel TEXT,
                    timestamp TIMESTAMP
                );
            """)

            # Insert the extracted data
            cursor.execute(
                """
                INSERT INTO alerts (
                    title, account_name, monitoring_group,
                    assets_blocklisted, zone_names,
                    channel, timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
                """,
                (
                    data["title"],
                    data["account_name"],
                    data["monitoring_group"],
                    data["assets_blocklisted"],
                    data["zone_names"],
                    data["channel"],
                    float(data["timestamp"])
                )
            )
            conn.commit()
            print("✅ Inserted alert into Supabase DB.")
        except Exception as e:
            print("❌ Failed to insert data:", e)
            if conn:
                conn.rollback()
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

# Start the app
# FastAPI instance (for Koyeb health check)
api = FastAPI()

@api.get("/")
def root():
    return {"status": "Slack ETL bot is alive!"}

from multiprocessing import Process

def start_slack():
    try:
        print("👂 Listening to Slack events via Socket Mode...")
        SocketModeHandler(app, os.getenv("SLACK_APP_TOKEN")).start()
    except Exception as e:
        print("❌ Failed to start Slack SocketModeHandler:", e)

if __name__ == "__main__":
    # Start Slack listener in a separate process
    slack_process = Process(target=start_slack)
    slack_process.start()

    # Run FastAPI server
    print("🚀 Starting FastAPI server...")
    uvicorn.run("app:api", host="0.0.0.0", port=3000)

    # Wait for Slack process to finish
    slack_process.join()

