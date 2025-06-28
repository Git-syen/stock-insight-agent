import os
from notion_client import Client
from datetime import datetime

notion = Client(auth=os.getenv("NOTION_TOKEN"))
db_id = os.getenv("NOTION_DB_ID")

def update_notion(filter_name, file_url):
    pages = notion.databases.query(database_id=db_id)
    for page in pages["results"]:
        name = page["properties"]["Filter Type"]["title"][0]["text"]["content"]
        if name == filter_name:
            notion.pages.update(
                page_id=page["id"],
                properties={
                    "Result Link": {"url": file_url},
                    "Last Run": {"date": {"start": datetime.today().isoformat()[:10]}}
                }
            )