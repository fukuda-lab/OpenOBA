import asyncio
import os
import random
import sqlite3
import sys
from pathlib import Path
from typing import Literal, Optional

import aiohttp
from tqdm import tqdm
from tranco import Tranco

from .categorizer import Categorizer
from .enums import IAB_CATEGORIES, TrainingPagesQueries

# sys.path.append("../openwpm")
# from bannerclick.config import WATCHDOG

# from fobam.openwpm.config import BrowserParams, ManagerParams
# from fobam.openwpm.storage.sql_provider import SQLiteStorageProvider
# from fobam.openwpm.task_manager import TaskManager  # docs: https://pypi.org/project/tranco/


# from datetime import datetime
# today_date = datetime.today().strftime('%Y-%m-%d')
DEFAULT_N_PAGES = 10000


class TrainingPagesHandler:
    # TODO: Discuss confidence level for WebShrinker categorization, wether we should use only confident categories for pages (reducing the amount of "usable" pages collected "automatically", but making sure they fit the category) or if we should use all the categories (increasing the amount of "usable" pages collected "automatically", but making sure they fit the category)
    """Handler for the Training Pages generation, processing and reading logic. The name for the db is 'list_id-n_pages'"""

    def __init__(
        self,
        list_id: str = "",
        categorize: bool = False,
        webshrinker_credentials: dict[str, str] = None,
        n_pages: int = DEFAULT_N_PAGES,
        updated_tranco: bool = False,
        custom_list: bool = False,
        custom_pages_list: list = [],
    ):
        if categorize and (
            not webshrinker_credentials
            or webshrinker_credentials.keys() != {"api_key", "secret_key"}
        ):
            raise ValueError(
                "When using the categorization feature, valid credentials for"
                " Webshrinker need to be provided"
            )
        self.n_pages = n_pages
        if webshrinker_credentials:
            self.categorizer = Categorizer(**webshrinker_credentials)
        else:
            self.categorizer = None
        self.custom_list = custom_list

        # Case custom pages
        if custom_list:
            if not custom_pages_list or not list_id:
                raise ValueError(
                    "For custom training pages lists, an id for the list and a value"
                    " for the list of url strings must be provided"
                )
            self.training_pages_list = custom_pages_list
            self.list_id = f"custom-{list_id}"

        # Cases using tranco
        else:
            self.tranco = TrainingPagesHandler.retrieve_tranco_top_list(
                list_id=list_id, updated=updated_tranco
            )

            self.training_pages_list = self.tranco.top(n_pages)
            self.list_id = self.tranco.list_id

        # This way we make sure that we never create the exact same database two times for a given list with a fixed number of pages
        self.sqlite_db_name = (
            f"{self.list_id}-{str(len(self.training_pages_list))}.sqlite"
        )
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(
            script_dir, "datadir_training_pages", self.sqlite_db_name
        )

    @staticmethod
    def retrieve_tranco_top_list(
        list_id: str = "",
        updated: bool = False,
        # list_date : str = '',
    ):
        """Call Tranco library list and get the pages either from cache or up-to-date.
        Returns a TrancoList object with method top(n_pages) and attributes list_id and date.
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        tranco_cache_path = os.path.join(script_dir, "tranco_lists")

        t = Tranco(cache=True, cache_dir=tranco_cache_path)
        if updated == True:
            # Get latest
            tranco_list = t.list()
        if updated == False:
            tranco_list = t.list(list_id=list_id)

        return tranco_list

    def _create_db_if_not_exists(self):
        """Creates the database for the training_pages related data, and insert the training pages in the TrainingPages table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS TrainingPages (
            id INTEGER PRIMARY KEY,
            page_url TEXT UNIQUE,
            tranco_rank INTEGER DEFAULT NULL,
            cookie_banner_checked	BOOLEAN DEFAULT 0,
            cookie_banner_presence BOOLEAN DEFAULT NULL,
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS RetrievedCategories (
            id INTEGER PRIMARY KEY,
            training_page_id INTEGER,
            training_page_url INTEGER,
            category TEXT,
            taxonomy TEXT,
            taxonomy_tier INTEGER,
            taxonomy_id TEXT,
            confident BOOLEAN,
            FOREIGN KEY(training_page_id) REFERENCES TrainingPages(id)
        )
        """)

        for index, page in enumerate(self.training_pages_list):
            if self.custom_list:
                print(
                    "Remember that custom pages must include http protocol in the"
                    " string"
                )
                # If it is a custom list, we cannot add the tranco_rank to the training page entry
                try:
                    cursor.execute(
                        "INSERT INTO TrainingPages (page_url) VALUES (?)", (page)
                    )
                except sqlite3.IntegrityError:
                    print(
                        "[DUPLICATE ERROR] Possible that page_url: {page} already"
                        " exists"
                    )
                    pass
            else:
                url = "http://" + page
                try:
                    cursor.execute(
                        "INSERT INTO TrainingPages (page_url, tranco_rank) VALUES"
                        " (?, ?)",
                        (url, index + 1),
                    )
                except sqlite3.IntegrityError:
                    print(
                        "[DUPLICATE ERROR] Possible that page_url: {url} already exists"
                    )
                    pass

        conn.commit()
        conn.close()
        return

    def categorize_training_pages(self, request_rate: int = 1, taxonomy: str = "iabv1"):
        """Initializes a new or existing SQLite database with the training_pages list, and starts/resumes the categorization of the list"""

        def _get_uncategorized_pages(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # Query to retrieve the list of TrainingPages without categories
            cursor.execute("""
                SELECT TrainingPages.page_url
                FROM TrainingPages
                LEFT JOIN RetrievedCategories ON TrainingPages.id = RetrievedCategories.training_page_id
                WHERE RetrievedCategories.id IS NULL
            """)
            result = cursor.fetchall()
            page_urls_without_categories = [row[0] for row in result]

            print(f"{len(page_urls_without_categories)} pages yet to be categorized...")
            return page_urls_without_categories

        async def _async_categorize(db_path, request_rate, urls_to_categorize):
            """Categorize a list of URLs asyncronously given a request rate"""
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            async with aiohttp.ClientSession() as session:
                progress = tqdm(total=len(urls_to_categorize), desc="Categorizing")
                sem = asyncio.Semaphore(request_rate)

                async def fetch_and_store(url):
                    async with sem:
                        # TODO: Ask about limiting the number of retries
                        # retries = 3  # Number of retries
                        delay = 1  # Initial delay in seconds
                        while True:
                            response = await self.categorizer.categorize(
                                session, url, taxonomy=taxonomy
                            )
                            if response["status_code"] == 200:
                                # TODO: check case where we would need more than one "top_level_category"
                                # Poblate RetrievedCategories with all the categories received (max 3 per url in case of normal sites url with WebShrinker taxonomy)
                                for result in response["categories_response"]:
                                    cursor.execute(
                                        "SELECT id FROM TrainingPages WHERE page_url=?",
                                        (url,),
                                    )
                                    page_id = cursor.fetchone()[0]
                                    cursor.execute(
                                        "INSERT INTO RetrievedCategories"
                                        " (training_page_id, training_page_url,"
                                        " category, taxonomy, taxonomy_tier,"
                                        " taxonomy_id, confident) VALUES (?, ?, ?, ?,"
                                        " ?, ?, ?)",
                                        (
                                            page_id,
                                            url,
                                            result["category"],
                                            result["taxonomy"],
                                            result["taxonomy_tier"],
                                            result["taxonomy_id"],
                                            result["confident"],
                                        ),
                                    )
                                progress.update(1)
                                print(f"[SUCCESS] Fetched categories for {url}")
                                break
                            elif response["status_code"] == 429:
                                print(
                                    f"[WARNING] Rate limit exceeded for {url}. Retrying"
                                    f" in {delay} seconds..."
                                )
                                await asyncio.sleep(delay)
                                if delay < 4:
                                    delay *= 2  # Double the delay for each retry
                                else:
                                    delay += (
                                        1  # Add only one second after for 4 seconds
                                    )

                            else:
                                print(f"[ERROR] {response}")
                                break

                tasks = [fetch_and_store(url) for url in urls_to_categorize]
                await asyncio.gather(*tasks)

                conn.commit()
                conn.close()

        self._create_db_if_not_exists()

        # Only categorize missing training_pages so we can just resume in case of errors or large scale lists
        uncategorized_training_pages = _get_uncategorized_pages(self.db_path)
        if len(uncategorized_training_pages) == 0:
            print(
                "All training pages are already categorized or the list_id + number of"
                " pages is incorrect (db_name error)"
            )
            return
        asyncio.run(
            _async_categorize(self.db_path, request_rate, uncategorized_training_pages)
        )

    def get_training_pages_grouped_by_category(
        self,
        k: int = 10,
        confident: bool = None,
        # This argument is for pages that have been crawled for cookie banner presence (1. True if it was checked, 2. False if it wasn't checked, 3. None for both cases)
        cookie_banner_checked: bool = None,
        # This argument is for the value of the cookie banner presence (1. True if it was checked and a cookie banner was found, 2. False if it was checked and cookie banner wasn't found, 3. None for both cases)
        cookie_banner_presence: bool = None,
    ):
        """Returns at most the k most popular pages by category given they are already categorized in the database (by id). Can be filtered by confident and cookie_banner_presence"""
        print(self.db_path)

        if (
            not cookie_banner_checked or cookie_banner_checked is None
        ) and cookie_banner_presence is not None:
            raise ValueError(
                "When filtering by cookie_banner_presence, cookie_banner_checked must"
                " be true, since it is the only way to know if the page has been"
                " crawled for cookie banner presence"
            )

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if confident is not None:
            # Case confident filter activated:
            if cookie_banner_checked is not None:
                # Case confident and cookie banner checked filter activated
                if cookie_banner_presence is not None:
                    # Case confident, cookie banner checked and cookie banner found filter activated (all filters)
                    query = (
                        TrainingPagesQueries.SelectTrainingPagesWithCategoryAllFilters
                    )
                else:
                    # Case ONLY confident and cookie banner checked filter activated
                    query = (
                        TrainingPagesQueries.SelectTrainingPagesWithCategoryConfidentAndCBCheckedFilter
                    )
            else:
                # Case ONLY confident filter activated
                query = (
                    TrainingPagesQueries.SelectTrainingPagesWithCategoryConfidentFilter
                )
        else:
            # Case confident filter NOT activated:
            if cookie_banner_checked is not None:
                # Case cookie banner checked filter activated
                if cookie_banner_presence is not None:
                    # Case cookie banner checked and cookie banner found filter activated (both cookie banner filters, no confident filter)
                    query = (
                        TrainingPagesQueries.SelectTrainingPagesWithCategoryCBCheckedAndPresenceFilter
                    )
                else:
                    # Case ONLY cookie banner checked filter activated
                    query = (
                        TrainingPagesQueries.SelectTrainingPagesWithCategoryCBCheckedFilter
                    )
            else:
                # Case NO filter activated
                query = TrainingPagesQueries.SelectTrainingPagesWithCategoryNoFilter

        # VALUES TO SQLITE BOOLEANS AND CONNECT TO DATABASE
        confident = 1 if confident else 0
        cookie_banner_checked = 1 if cookie_banner_checked else 0
        cookie_banner_presence = 1 if cookie_banner_presence else 0

        # Execute query
        cursor.execute(
            query,
            {
                "k": k,
                "confident": confident,
                "cookie_banner_checked": cookie_banner_checked,
                "cookie_banner_presence": cookie_banner_presence,
            },
        )

        rows = cursor.fetchall()
        # Close connection
        conn.close()

        # Build dict with key = category, value = dict with keys = category, top_lowest_id_pages, top_page_urls, total_pages
        categories_dict = {
            category_t2_name: {
                "pages_urls": [],
                "pages_ids": [],
            }
            for category_t1_dict in IAB_CATEGORIES.values()
            for category_t2_name in category_t1_dict.values()
        }
        for row in rows:
            categories_dict[row[0]] = {
                "pages_urls": row[2].strip().split(", "),
                "pages_ids": [
                    int(training_page_id)
                    for training_page_id in row[1].strip().split(", ")
                ],
            }
        return categories_dict

    def generate_training_pages_summary(self):

        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Fetch the total number of training pages
        cursor.execute("SELECT COUNT(*) FROM TrainingPages")
        total_training_pages = cursor.fetchone()[0]
        print(f"Total training pages: {total_training_pages}")

        # Fetch the number of categorized training pages
        cursor.execute("""
        SELECT COUNT(DISTINCT training_page_id)
        FROM RetrievedCategories
        """)
        total_categorized_pages = cursor.fetchone()[0]
        print(f"Total categorized training pages: {total_categorized_pages}")

        # Fetch a few categorized pages as samples
        cursor.execute("""
        SELECT id, training_page_url, category, 
            taxonomy, taxonomy_tier, taxonomy_id,
            confident, training_page_id
        FROM RetrievedCategories
        ORDER BY category
        """)
        samples = cursor.fetchall()

        print("\nSample categorized pages:")
        for sample in samples:
            print(
                f"ID: {sample[0]}\nURL: {sample[1]}\nCategory: {sample[2]}\nTaxonomy:"
                f" {sample[3]}\nTaxonomy Tier: {sample[4]}\n"
            )

        # Close the connection
        conn.close()

    def _validate_urls_in_db(self, page_urls):
        # Fetch
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                page_url 
            FROM 
                TrainingPages
            """)
        rows = cursor.fetchall()
        conn.close()

        # Build list of URLs
        urls = [row[0] for row in rows]

        # Validate URLs are in the database
        for url in page_urls:
            if url not in urls:
                raise ValueError(f"URL {url} not found in database")


# API_KEY = 'GhU39K7bdfvdxRlcnEkT'
# SECRET_KEY = 'ZwnCzHIpw08DF10Fmz5c'
# # Example usage
# credentials = {
#     'api_key': API_KEY,
#     'secret_key': SECRET_KEY
# }
# # Custom list usage
# # training_pages_list = ['google.com', 'facebook.com']
# # pages_handler = TrainingPagesHandler(credentials, custom_list=True, list_id = 'test1', custom_pages_list=training_pages_list)

# # Tranco list usage
# pages_handler = TrainingPagesHandler(credentials, 'X53KN', n_pages = 10)
# pages_handler.categorize_training_pages()
# # print_db_summary(pages_handler.sqlite_db_name)

# test_category_pages = pages_handler.get_training_pages_by_category('iabv1', 'Technology & Computing')
# print(test_category_pages)
