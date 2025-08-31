import httpx
import typer
import json
import sqlite3
import time
from typing import Optional

app = typer.Typer()

collection_list_endpoint = "https://collections.mun.ca/digital/bl/dmwebservices/index.php?q=dmGetCollectionList/json"


@app.command()
def collections():
    """List all collections from the MUN Digital Archive."""
    try:
        response = httpx.get(collection_list_endpoint, verify=False)
        response.raise_for_status()
        collections_data = response.json()

        for collection in collections_data:
            print(f"Alias: {collection['alias']}")
            print(f"Name: {collection['name']}")
            print(f"Path: {collection['path']}")
            print(f"Secondary Alias: {collection['secondary_alias']}")
            print("-" * 50)

    except httpx.RequestError as e:
        typer.echo(f"Error fetching collections: {e}", err=True)
        raise typer.Exit(1)
    except json.JSONDecodeError as e:
        typer.echo(f"Error parsing JSON response: {e}", err=True)
        raise typer.Exit(1)


def init_database():
    """Initialize SQLite database with required schema."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scraped_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            year INTEGER NOT NULL,
            page INTEGER NOT NULL,
            title TEXT NOT NULL,
            item_link TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def save_items_to_db(url: str, year: int, page: int, items: list):
    """Save scraped items to SQLite database."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    for item in items:
        cursor.execute(
            """
            INSERT INTO scraped_data (url, year, page, title, item_link)
            VALUES (?, ?, ?, ?, ?)
        """,
            (url, year, page, item["title"], item["itemLink"]),
        )

    conn.commit()
    conn.close()


def get_scraped_pages(year: int) -> set:
    """Get set of already scraped pages for a given year."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT page FROM scraped_data WHERE year = ?", (year,))
    pages = {row[0] for row in cursor.fetchall()}

    conn.close()
    return pages


def fetch_with_retry(url: str, max_retries: int = 3, delay: float = 1.0):
    """Fetch URL with retry logic for handling 502 errors."""
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, verify=False, timeout=30.0)

            # If we get a 502, retry
            if response.status_code == 502:
                if attempt < max_retries - 1:
                    typer.echo(
                        f"    502 error (attempt {attempt + 1}/{max_retries}), retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                    continue
                else:
                    response.raise_for_status()

            response.raise_for_status()
            return response

        except httpx.RequestError as e:
            if attempt < max_retries - 1:
                typer.echo(
                    f"    Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {delay}s..."
                )
                time.sleep(delay)
                delay *= 2
                continue
            else:
                raise e


@app.command()
def scrape(
    start_year: int = typer.Option(1950, help="Starting year to scrape"),
    end_year: int = typer.Option(2017, help="Ending year to scrape"),
    resume: bool = typer.Option(True, help="Resume from where we left off"),
):
    """Scrape MUSE data from start_year to end_year and save to SQLite database."""
    init_database()

    base_url = "https://collections.mun.ca/digital/api/search/collection/muse/searchterm/{year}/field/subcol/mode/exact/conn/and/order/title/ad//maxRecords/50"

    total_items = 0

    for year in range(start_year, end_year + 1):
        typer.echo(f"Scraping year {year}...")

        # Check for existing pages if resuming
        scraped_pages = get_scraped_pages(year) if resume else set()
        if scraped_pages:
            typer.echo(f"  Found existing pages: {sorted(scraped_pages)}")

        page = 1
        year_items = 0

        while True:
            # Skip already scraped pages
            if resume and page in scraped_pages:
                typer.echo(f"  Page {page}: skipping (already scraped)")
                page += 1
                continue

            if page == 1:
                url = base_url.format(year=year)
            else:
                url = base_url.format(year=year) + f"/page/{page}"

            try:
                response = fetch_with_retry(url)
                data = response.json()

                items = data.get("items", [])
                num_items = len(items)

                if num_items == 0:
                    if page == 1:
                        typer.echo(f"  No items found for year {year}")
                    break

                # Save items to database
                save_items_to_db(url, year, page, items)

                year_items += num_items
                total_items += num_items

                typer.echo(f"  Page {page}: {num_items} items (saved)")

                # If we got less than 50 items, we're done with this year
                if num_items < 50:
                    break

                page += 1
                # Small delay to be respectful to the server
                time.sleep(0.2)

            except httpx.RequestError as e:
                typer.echo(f"Error fetching {url}: {e}", err=True)
                typer.echo(
                    f"Stopping at year {year}, page {page}. You can resume later.",
                    err=True,
                )
                break
            except json.JSONDecodeError as e:
                typer.echo(f"Error parsing JSON from {url}: {e}", err=True)
                typer.echo(
                    f"Stopping at year {year}, page {page}. You can resume later.",
                    err=True,
                )
                break

        # Count items from database for this year (including previously scraped)
        conn = sqlite3.connect("muse_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM scraped_data WHERE year = ?", (year,))
        total_year_items = cursor.fetchone()[0]
        conn.close()

        typer.echo(
            f"  Year {year} complete: {total_year_items} total items ({year_items} new)"
        )

    # Final count from database
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM scraped_data")
    final_total = cursor.fetchone()[0]
    conn.close()

    typer.echo(f"\nScraping complete! Total items in database: {final_total}")
    typer.echo("Data saved to muse_data.db")


if __name__ == "__main__":
    app()
