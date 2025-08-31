import httpx
import typer
import json
import sqlite3
import time
from typing import Optional
from pathlib import Path
import re

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
            date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS article_pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_data_id INTEGER NOT NULL,
            article_id INTEGER NOT NULL,
            page_id INTEGER NOT NULL,
            page_title TEXT NOT NULL,
            page_number INTEGER,
            thumbnail_uri TEXT,
            date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (scraped_data_id) REFERENCES scraped_data (id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS download_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER NOT NULL UNIQUE,
            year INTEGER NOT NULL,
            title TEXT NOT NULL,
            date TEXT,
            page_title TEXT NOT NULL,
            page_number INTEGER,
            file_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (page_id) REFERENCES article_pages (page_id)
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
            INSERT INTO scraped_data (url, year, page, title, item_link, date)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (url, year, page, item["title"], item["itemLink"], None),
        )

    conn.commit()
    conn.close()


def create_safe_filename(title: str, page_number: int) -> str:
    """Create a safe filename from page title with numbering for sorting."""
    # Clean the title for filesystem
    clean_title = re.sub(r"[^\w\s-]", "", title)
    clean_title = re.sub(r"[-\s]+", "_", clean_title).strip("_")

    # Format with leading zero for proper sorting
    return f"{page_number:02d}_{clean_title}"


def download_image(url: str, filepath: Path, max_retries: int = 3) -> bool:
    """Download an image with retry logic."""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(max_retries):
        try:
            response = httpx.get(url, verify=False, timeout=60.0)

            if response.status_code == 200:
                with open(filepath, "wb") as f:
                    f.write(response.content)
                return True
            elif response.status_code == 404:
                return False  # Don't retry 404s
            else:
                if attempt < max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return False

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))
                continue
            return False

    return False


def save_article_pages(scraped_data_id: int, article_data: dict):
    """Save article pages data to SQLite database."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    parent = article_data["parent"]
    article_id = article_data["requestedId"]

    # Extract date from parent fields
    date_value = None
    for field in parent["fields"]:
        if field["key"] == "date":
            date_value = field["value"]
            break

    # Update the scraped_data record with the date
    cursor.execute(
        "UPDATE scraped_data SET date = ? WHERE id = ?", (date_value, scraped_data_id)
    )

    # Save each child page
    for child in parent["children"]:
        page_number = None
        if child["title"].startswith("Page "):
            try:
                page_number = int(child["title"].split(" ")[1])
            except (IndexError, ValueError):
                pass
        elif child["title"] == "Cover":
            page_number = 1

        cursor.execute(
            """
            INSERT INTO article_pages (scraped_data_id, article_id, page_id, page_title, page_number, thumbnail_uri, date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scraped_data_id,
                article_id,
                child["id"],
                child["title"],
                page_number,
                child["thumbnailUri"],
                date_value,
            ),
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


def init_download_progress(year_filter: Optional[int] = None):
    """Initialize download progress tracking for pending images."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    # Build query to get all pages that need downloading
    query = """
        SELECT s.title, s.date, s.year, ap.page_id, ap.page_title, ap.page_number
        FROM scraped_data s
        JOIN article_pages ap ON s.id = ap.scraped_data_id
        WHERE ap.page_number IS NOT NULL
    """
    params = ()

    if year_filter:
        query += " AND s.year = ?"
        params = (year_filter,)

    query += " ORDER BY s.year, s.date, ap.page_number"

    cursor.execute(query, params)
    pages = cursor.fetchall()

    # Create download progress entries for new pages
    for title, date, year, page_id, page_title, page_number in pages:
        # Create file path structure
        safe_title = re.sub(r"[^\w\s-]", "", title)
        safe_title = re.sub(r"[-\s]+", "_", safe_title).strip("_")
        folder_name = f"{date}_{safe_title}"
        filename = create_safe_filename(page_title, page_number)
        file_path = f"{year}/{folder_name}/{filename}.jpg"

        # Insert if not already exists
        cursor.execute(
            """
            INSERT OR IGNORE INTO download_progress 
            (page_id, year, title, date, page_title, page_number, file_path, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        """,
            (page_id, year, title, date, page_title, page_number, file_path),
        )

    conn.commit()
    conn.close()


def mark_download_complete(page_id: int):
    """Mark a download as complete."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE download_progress 
        SET status = 'completed', completed_at = CURRENT_TIMESTAMP 
        WHERE page_id = ?
    """,
        (page_id,),
    )

    conn.commit()
    conn.close()


def mark_download_failed(page_id: int):
    """Mark a download as failed."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE download_progress 
        SET status = 'failed', completed_at = CURRENT_TIMESTAMP 
        WHERE page_id = ?
    """,
        (page_id,),
    )

    conn.commit()
    conn.close()


def get_pending_downloads(year_filter: Optional[int] = None):
    """Get all pending downloads."""
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    query = """
        SELECT page_id, year, title, date, page_title, page_number, file_path
        FROM download_progress 
        WHERE status = 'pending'
    """
    params = ()

    if year_filter:
        query += " AND year = ?"
        params = (year_filter,)

    query += " ORDER BY year, date, page_number"

    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()

    return results


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


@app.command()
def scrape_pages(
    resume: bool = typer.Option(True, help="Resume from where we left off"),
):
    """Scrape individual page data for all articles in the database."""
    init_database()

    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    # Get all articles that don't have page data yet
    cursor.execute("""
        SELECT s.id, s.item_link, s.title, s.year
        FROM scraped_data s
        LEFT JOIN article_pages ap ON s.id = ap.scraped_data_id
        WHERE ap.scraped_data_id IS NULL
        ORDER BY s.year, s.id
    """)
    articles = cursor.fetchall()

    # Get count of already processed articles for progress tracking
    cursor.execute("""
        SELECT COUNT(DISTINCT scraped_data_id) FROM article_pages
    """)
    already_processed = cursor.fetchone()[0]

    conn.close()

    total_articles = len(articles)
    if resume and already_processed > 0:
        typer.echo(f"Found {already_processed} articles already processed")

    typer.echo(f"Found {total_articles} articles remaining to process...")

    processed = 0
    for article_id, item_link, title, year in articles:
        # Extract numerical ID from item_link
        numerical_id = item_link.split("/id/")[1]

        api_url = f"https://collections.mun.ca/digital/api/collections/muse/items/{numerical_id}/false"

        try:
            typer.echo(
                f"Processing {year} - {title[:50]}... ({processed + 1}/{total_articles})"
            )
            response = fetch_with_retry(api_url)
            data = response.json()

            # Save article pages data
            save_article_pages(article_id, data)

            processed += 1

            # Small delay to be respectful to the server
            time.sleep(0.3)

        except KeyboardInterrupt:
            typer.echo(
                f"\nInterrupted! Processed {processed} articles in this session."
            )
            typer.echo(f"Run the command again to resume from where you left off.")
            raise typer.Exit(0)
        except Exception as e:
            typer.echo(f"Error processing article {numerical_id}: {e}", err=True)
            continue

    # Final count
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM article_pages")
    total_pages = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT scraped_data_id) FROM article_pages")
    articles_with_pages = cursor.fetchone()[0]
    conn.close()

    typer.echo(f"\nPage scraping complete!")
    typer.echo(f"Articles processed: {articles_with_pages}")
    typer.echo(f"Total pages found: {total_pages}")


@app.command()
def download_images(
    output_dir: str = typer.Option("images", help="Directory to save images"),
    year_filter: Optional[int] = typer.Option(
        None, help="Only download images for specific year"
    ),
    resume: bool = typer.Option(True, help="Resume from where we left off"),
):
    """Download all page images and organize them into folders."""
    init_database()
    base_path = Path(output_dir)
    base_path.mkdir(exist_ok=True)

    # Initialize download progress tracking
    init_download_progress(year_filter)

    # Get pending downloads
    pending_downloads = get_pending_downloads(year_filter)

    # Check for existing files and update status if resuming
    if resume:
        conn = sqlite3.connect("muse_data.db")
        cursor = conn.cursor()

        updated_count = 0
        for (
            page_id,
            year,
            title,
            date,
            page_title,
            page_number,
            file_path,
        ) in pending_downloads:
            image_path = base_path / file_path
            if image_path.exists():
                mark_download_complete(page_id)
                updated_count += 1

        conn.close()

        if updated_count > 0:
            typer.echo(
                f"Found {updated_count} files already downloaded, marked as complete"
            )
            # Refresh pending downloads list
            pending_downloads = get_pending_downloads(year_filter)

    total_pages = len(pending_downloads)
    typer.echo(f"Found {total_pages} pages remaining to download...")

    if total_pages == 0:
        typer.echo("All images already downloaded!")
        return

    downloaded = 0
    failed = 0

    current_article = None

    for (
        page_id,
        year,
        title,
        date,
        page_title,
        page_number,
        file_path,
    ) in pending_downloads:
        # Show progress for new articles
        if current_article != (title, date):
            current_article = (title, date)
            typer.echo(f"Processing {year} - {title[:50]}...")

        # Create full image path
        image_path = base_path / file_path

        # Download image
        image_url = f"https://collections.mun.ca/digital/api/singleitem/image/muse/{page_id}/default.jpg"

        try:
            if download_image(image_url, image_path):
                downloaded += 1
                mark_download_complete(page_id)
                if downloaded % 10 == 0:
                    typer.echo(f"  Downloaded {downloaded}/{total_pages} images...")
            else:
                failed += 1
                mark_download_failed(page_id)
                typer.echo(f"  Failed to download {page_id} ({page_title})")

            # Small delay to be respectful
            time.sleep(0.1)

        except KeyboardInterrupt:
            typer.echo(
                f"\nInterrupted! Downloaded {downloaded} images in this session."
            )
            typer.echo(f"Run the command again to resume from where you left off.")
            raise typer.Exit(0)

    # Final stats
    conn = sqlite3.connect("muse_data.db")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM download_progress WHERE status = 'completed'")
    total_completed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM download_progress WHERE status = 'failed'")
    total_failed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM download_progress WHERE status = 'pending'")
    total_pending = cursor.fetchone()[0]

    conn.close()

    typer.echo(f"\nImage download session complete!")
    typer.echo(f"Downloaded this session: {downloaded}")
    typer.echo(f"Failed this session: {failed}")
    typer.echo(f"Total completed: {total_completed}")
    typer.echo(f"Total failed: {total_failed}")
    typer.echo(f"Total pending: {total_pending}")
    typer.echo(f"Images saved to: {base_path.absolute()}")


if __name__ == "__main__":
    app()
