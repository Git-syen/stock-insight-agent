import os
from notion_client import Client
from datetime import datetime

notion = Client(auth=os.getenv("NOTION_TOKEN"))
main_db_id = os.getenv("NOTION_DB_ID")

# Mapping filter names to their respective sub-table database IDs
FILTER_TABLES = {
    "Momentum Stocks": os.getenv("NOTION_DB_MOMENTUM"),
    "RS Outperformers": os.getenv("NOTION_DB_RS"),
    "Accumulating Stocks": os.getenv("NOTION_DB_ACCUM"),
    "Price Action Volume Spike": os.getenv("NOTION_DB_PV"),
    "Multi-Factor Picks": os.getenv("NOTION_DB_MULTI"),
    # Add weekly filters if needed
}

def update_notion(filter_name, file_url, preview_df=None):
    try:
        pages = notion.databases.query(database_id=main_db_id)
    except Exception as e:
        print(f"Error fetching main dashboard from Notion: {e}")
        return

    # Update main dashboard entry
    for page in pages.get("results", []):
        try:
            title_property = page["properties"].get("Filter Type", {}).get("title", [])
            if not title_property:
                continue
            name = title_property[0]["text"]["content"]

            if name.strip().lower() == filter_name.strip().lower():
                update_payload = {
                    "Result Link": {"url": file_url},
                    "Last Run": {"date": {"start": datetime.today().isoformat()[:10]}}
                }
                notion.pages.update(page_id=page["id"], properties=update_payload)
                break
        except Exception as e:
            print(f"⚠️ Error updating main dashboard for '{filter_name}': {e}")

    # Push preview table to sub-database if available
    db_id = FILTER_TABLES.get(filter_name)
    if preview_df is not None and db_id:
        try:
            # Optional: delete old rows before inserting new data
            existing = notion.databases.query(database_id=db_id)
            for row in existing["results"]:
                notion.blocks.delete(block_id=row["id"])
        except Exception as e:
            print(f"⚠️ Couldn't clear existing rows for '{filter_name}': {e}")

        for _, row in preview_df.iterrows():
            properties = {}
            for col in preview_df.columns:
                val = str(row[col])
                if col.lower() == "symbol":
                    properties["Name"] = {
                        "title": [{"text": {"content": val}}]
                    }
                else:
                    properties[col] = {
                        "rich_text": [{"text": {"content": val}}]
                    }

            try:
                notion.pages.create(
                    parent={"database_id": db_id},
                    properties=properties
                )
            except Exception as e:
                print(f"⚠️ Error adding row to '{filter_name}': {e}")
