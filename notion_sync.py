import os
from notion_client import Client
from datetime import datetime

notion = Client(auth=os.getenv("NOTION_TOKEN"))
db_id = os.getenv("NOTION_DB_ID")

def update_notion(filter_name, file_url):
    try:
        pages = notion.databases.query(database_id=db_id)
    except Exception as e:
        print(f"Error fetching pages from Notion: {e}")
        return

    for page in pages.get("results", []):
        try:
            title_property = page["properties"].get("Filter Type", {}).get("title", [])
            if not title_property:
                continue
            name = title_property[0]["text"]["content"]

            if name.strip().lower() == filter_name.strip().lower():
                notion.pages.update(
                    page_id=page["id"],
                    properties={
                        "Result Link": {"url": file_url},
                        "Last Run": {"date": {"start": datetime.today().isoformat()[:10]}}
                    }
                )
                print(f"✅ Updated Notion page for '{filter_name}'")
                return

        except Exception as e:
            print(f"⚠️ Error updating page for '{filter_name}': {e}")
