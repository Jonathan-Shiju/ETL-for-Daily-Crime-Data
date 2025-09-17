from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field
import psycopg2
import os


class RssScraperInput(BaseModel):
    urls: list[str] = Field(..., description="List of RSS feed URLs.")

class RssScraper(BaseTool):
    name: str = "RSS Scraper"
    description: str = (
        "Fetches raw XML data from provided RSS feed URLs and returns it."
    )
    args_schema: Type[BaseModel] = RssScraperInput

    def _run(self, urls: list[str]) -> list[dict]:
        import requests
        xml_data = []
        for url in urls:
            response = requests.get(url)
            xml_data.append({
                "url": url,
                "xml": response.text
            })
        return xml_data

class PostgresLoaderInput(BaseModel):
    articles: list[dict] = Field(..., description="List of structured article JSON objects.")
    # db_url removed

class PostgresLoader(BaseTool):
    name: str = "PostgreSQL Loader"
    description: str = (
        "Loads structured crime article JSON objects into a PostgreSQL database."
    )
    args_schema: Type[BaseModel] = PostgresLoaderInput

    def _run(self, articles: list[dict]) -> str:
        db_url = os.environ.get("POSTGRES_DB_URL")
        if not db_url:
            raise ValueError("POSTGRES_DB_URL environment variable not set.")
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            for article in articles:
                cur.execute(
                    """
                    INSERT INTO crime_articles (title, involved_parties, date_of_incident, location_of_incident, severity_of_crime, image_url, additional_links)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        article.get("title"),
                        article.get("involved_parties"),
                        article.get("date_of_incident"),
                        article.get("location_of_incident"),
                        article.get("severity_of_crime"),
                        article.get("image_url"),
                        article.get("additional_links"),
                    )
                )
            conn.commit()
            return f"Inserted {len(articles)} articles into PostgreSQL."
        except Exception as e:
            if conn:
                conn.rollback()
            return f"Error inserting articles: {str(e)}"
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
