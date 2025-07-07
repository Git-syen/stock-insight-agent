import os
from notion_client import Client
from datetime import datetime

notion = Client(auth=os.getenv("NOTION_TOKEN"))
db_id = os.getenv("NOTION_DB_ID")

def update_notion(filter_name, file_url, preview_df=None):
    try:
        pages = notion.databases.query(database_id=db_id)
    except Exception as e:
        print(f"Error fetching pages from Notion: {e}")
        return

    # Generate markdown preview (first 10 rows)
    table_text = ""
    if preview_df is not None and not preview_df.empty:
        preview = preview_df.head(10).copy()
        preview.columns = [col[:10] for col in preview.columns]  # Optional: truncate long column names
        table_text = preview.to_markdown(index=False)

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

                if table_text:
                    update_payload["Table Preview"] = {
                        "rich_text": [{"text": {"content": table_text[:2000]}}]  # Notion API text limit safety
                    }

                notion.pages.update(
                    page_id=page["id"],
                    properties=update_payload
                )
                print(f"✅ Updated Notion page for '{filter_name}'")
                return

        except Exception as e:
            print(f"⚠️ Error updating page for '{filter_name}': {e}")
