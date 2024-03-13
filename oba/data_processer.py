import asyncio
import glob
import json
import logging
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import AnyStr, DefaultDict, Dict, List, Optional, Set, Tuple

import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import UnexpectedAlertPresentException
import time

from openwpm.utilities.platform_utils import get_firefox_binary_path

import aiohttp
import tldextract

# from adblockparser import AdblockRules
from .categorizer import Categorizer

from .enums_data_processer import (
    VisitAdvertisementsQueries,
    LandingPageCategoriesQueries,
    LandingPagesQueries,
    OBABrowserQueries,
)

from .enums import IAB_CATEGORIES
from selenium.common.exceptions import TimeoutException

# from extract_ad_url import process

WEBSHRINKER_CREDENTIALS = {
    "api_key": "GhU39K7bdfvdxRlcnEkT",
    "secret_key": "ZwnCzHIpw08DF10Fmz5c",
}
DATA_FROM_VOLUME = False


class DataProcesser:
    # Create a logger
    logger = logging.getLogger(__name__)

    def __init__(self, experiment_name: str, webshrinker_credentials):
        self.experiment_name = experiment_name

        self.firefox_binary_path = get_firefox_binary_path()
        self.driver = self._setup_driver()

        if webshrinker_credentials:
            self.categorizer = Categorizer(**webshrinker_credentials)

        # Dirs, maybe would be better in a dictionary Paths
        self.source_pages_dir = f"../datadir/{self.experiment_name}/sources"

        if DATA_FROM_VOLUME:
            self.experiment_data_dir = f"/Volumes/FOBAM_data/28_02_style_and_fashion/datadir/{self.experiment_name}/"
        else:
            self.experiment_data_dir = f"datadir/{self.experiment_name}/"

        # self.static_ads = []
        # self.dynamic_ads = defaultdict(list)
        # self.control_site_urls = set()

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.binary_location = self.firefox_binary_path
        driver = webdriver.Firefox(options=options)
        driver.set_page_load_timeout(30)  # Set the timeout to 30 seconds
        return driver

    def get_ad_landing_page_url(self, ad_url):
        try:
            self.driver.get(ad_url)
            final_url = self._wait_for_url_stabilization()
            return final_url
        except TimeoutException:
            print(f"Page load timed out for URL: {ad_url}. Skipping...")
            return (
                ad_url  # Or return ad_url to indicate the original URL was not resolved
            )
        except Exception as e:
            print(f"An error occurred while loading the page: {e}")
            print("---------Default returning same ad_url...---------")
            return ad_url

    def _wait_for_url_stabilization(
        self, check_interval=1, stable_period=5, max_duration=30
    ):
        start_time = time.time()
        last_url = self.driver.current_url
        stable_since = start_time

        while time.time() - start_time < max_duration:
            time.sleep(check_interval)
            current_url = self.driver.current_url
            if current_url != last_url:
                last_url = current_url
                stable_since = time.time()
            elif time.time() - stable_since >= stable_period:
                # URL has been stable for the specified period; assume it's the final URL
                return current_url

        # Return the current URL if max_duration is reached without stabilization
        return self.driver.current_url

    def close_browser(self):
        self.driver.quit()

    def process_browsers_new_ads(
        self,
        crawled_data_cursor: sqlite3.Cursor,
        crawl_conn: sqlite3.Connection,
        oba_browser_ids,
        request_rate=1,
        taxonomy="iabv1",
    ):
        """Set dynamic ads, in a dictionary with all the ads by control_site and visit_id ordered by site_rank"""
        semaphore = asyncio.Semaphore(request_rate)

        async def _async_categorize_landing_page(
            landing_page_id: int, landing_page_url: str
        ):
            """Categorize the ad landing page with the Categorizer and save the categories in the AdvertisementsCategories table."""
            print(f"[ASYNC CATEGORIZE LANDING PAGE] {landing_page_url}")
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    response = await self.categorizer.categorize(
                        session, landing_page_url, taxonomy=taxonomy
                    )
                    delay = 1  # Initial delay in seconds
                    while True:
                        if response["status_code"] == 200:
                            # TODO: check case where we would need more than one "top_level_category"
                            print(
                                f"[CATEGORIES RESPONSE] {response['categories_response']}"
                            )
                            for result in response["categories_response"]:
                                # Get the parent category from the IAB_CATEGORIES dictionary
                                parent_category_code = result["taxonomy_id"].split("-")[
                                    0
                                ]
                                parent_category_name = IAB_CATEGORIES.get(
                                    parent_category_code
                                ).get(parent_category_code)

                                crawled_data_cursor.execute(
                                    LandingPageCategoriesQueries.InsertCategoryQuery,
                                    {
                                        "landing_page_id": landing_page_id,
                                        "landing_page_url": landing_page_url,
                                        "category_name": result["category"],
                                        "category_code": result["taxonomy_id"],
                                        "parent_category": parent_category_name,
                                        "confident": 1 if result["confident"] else 0,
                                    },
                                )
                            # Update the landing page with categorized = 1
                            crawled_data_cursor.execute(
                                LandingPagesQueries.UpdateLandingPageCategorizedQuery,
                                {"categorized": 1, "landing_page_id": landing_page_id},
                            )

                            # TODO: Implement progress bar
                            # progress.update(1)
                            print(
                                f"[SUCCESS] Fetched categories for {landing_page_url}"
                            )
                            break
                        elif response["status_code"] == 429:
                            print(
                                f"[WARNING] Rate limit exceeded for {landing_page_url}. Retrying in {delay} seconds..."
                            )
                            await asyncio.sleep(delay)
                            if delay < 4:
                                delay *= 2  # Double the delay for each retry
                            else:
                                delay += 1  # Add only one second after for 4 seconds

                        else:
                            print(f"[ERROR] {response}")
                            break

        visits_number = 0
        # Traverse all the browsers from the experiment
        for index, browser_id in enumerate(oba_browser_ids):
            print(
                f"[BROWSER ADS START] Starting crawled data dynamic ads extraction of browser {index + 1}/{len(oba_browser_ids)}..."
            )
            oba_browser_queries = OBABrowserQueries(browser_id=browser_id)

            # First retrieve all unresolved and uncategorized ads from the database
            crawled_data_cursor.execute(
                oba_browser_queries.get_unresolved_advertisements_query(),
                {"browser_id": browser_id},
            )
            unresolved_visit_ads = crawled_data_cursor.fetchall()

            # Get a list of unique ad URLs
            unique_unresolved_ad_urls = list(
                set([ad_url for _, ad_url in unresolved_visit_ads])
            )

            # Then resolve all the unique landing page URLs for the unresolved ads
            for ad_url in unique_unresolved_ad_urls:
                resolved_landing_page_url = None
                resolved_landing_page_id = None
                visits_number += 1
                print(
                    f"[UNRESOLVED AD URL START] {visits_number}/{len(unique_unresolved_ad_urls)} for {ad_url}"
                )
                start_time = time.time()
                # First check if the ad URL was already resolved previously in another visit advertisement
                crawled_data_cursor.execute(
                    VisitAdvertisementsQueries.SelectResolvedLandingPagesFromADURLQuery,
                    {
                        "visit_id": browser_id,
                        "browser_id": browser_id,
                        "ad_url": ad_url,
                    },
                )
                resolved_landing_pages_from_ad_url = crawled_data_cursor.fetchall()
                # If the ad URL was already resolved, use the resolved landing page URL
                for (
                    db_landing_page_id,
                    db_landing_page_url,
                ) in resolved_landing_pages_from_ad_url:
                    if db_landing_page_url != ad_url:
                        resolved_landing_page_url = db_landing_page_url
                        resolved_landing_page_id = db_landing_page_id
                        break

                if not resolved_landing_page_url:
                    resolved_landing_page_url = self.get_ad_landing_page_url(ad_url)
                #     if resolved_landing_page_url == ad_url:
                #         # If the resolved landing page URL is the same as the ad URL, skip the rest of the loop and continue to the next ad URL to leave it unresolved for later
                #         # TODO: mark something in the database to indicate that the ad URL is unresolved but has been attempted
                #         print(
                #             f"[UNRESOLVED AD URL] {ad_url} was not resolved. Skipping..."
                #         )
                #         continue

                finish_time = time.time()
                if finish_time - start_time > 5:
                    print(
                        f"[LONG TIME WARNING] Page load took {finish_time - start_time} seconds for URL: {ad_url}"
                    )
                print(
                    f"[RESOLVED LANDING PAGE URL] {resolved_landing_page_url} in {finish_time - start_time} seconds."
                )
                # Look for the resolved landing page URL in the landing_pages table
                crawled_data_cursor.execute(
                    LandingPagesQueries.SelectLandingPageFromURLQuery,
                    (resolved_landing_page_url,),
                )
                database_landing_page = crawled_data_cursor.fetchone()
                landing_page_categories = []

                # Look for the categories of the resolved landing page URL in the landing_page_categories table if it was already categorized
                if database_landing_page and database_landing_page[2]:
                    crawled_data_cursor.execute(
                        LandingPageCategoriesQueries.SelectCategoriesFromLandingPageURLQuery,
                        (resolved_landing_page_url,),
                    )
                    landing_page_categories = crawled_data_cursor.fetchall()

                try_for_second_time_condition = (
                    len(landing_page_categories) == 1
                    and landing_page_categories[0][0] == "Uncategorized"
                )

                # if the resolved landing page URL is not in the database, try to categorize it
                if (
                    not database_landing_page
                    or database_landing_page[2] == 0
                    or try_for_second_time_condition
                    # This condition is to check if the landing page has only been categorized as Uncategorized so we can try to categorize it again
                ):
                    if (
                        not database_landing_page
                    ):  # Don't add landing_url again if it's already in the database
                        # Insert the resolved landing page URL into the landing_pages table with categorized = 0 to retrieve the landing_page_id (needed for the landing_page_categories table)
                        crawled_data_cursor.execute(
                            LandingPagesQueries.InsertLandingPageQuery,
                            (resolved_landing_page_url, 0),
                        )
                    # Get the landing_page_id of the inserted landing page
                    crawled_data_cursor.execute(
                        LandingPagesQueries.SelectLandingPageFromURLQuery,
                        (resolved_landing_page_url,),
                    )
                    landing_page_id, _, _ = crawled_data_cursor.fetchone()
                    # Categorize the resolved landing page URL
                    asyncio.run(
                        _async_categorize_landing_page(
                            landing_page_id, resolved_landing_page_url
                        )
                    )
                else:
                    # If the resolved landing page URL is already in the database, retrieve the landing_page_id
                    landing_page_id, _, _ = database_landing_page
                    print("[ALREADY CATEGORIZED] Skipping categorization...")

                # Check one more time if the resolved landing page URL was categorized
                crawled_data_cursor.execute(
                    LandingPagesQueries.SelectLandingPageFromURLQuery,
                    (resolved_landing_page_url,),
                )
                _, _, landing_page_categorized = crawled_data_cursor.fetchone()

                # Update all the unresolved visit_advertisements that have the same ad_url with the resolved landing page URL and landing_page_id
                crawled_data_cursor.execute(
                    VisitAdvertisementsQueries.UpdateVisitAdvertisementLandingPageQuery,
                    {
                        "ad_url": ad_url,
                        "landing_page_url": resolved_landing_page_url,
                        "landing_page_id": landing_page_id,
                        "categorized": landing_page_categorized,
                    },
                )

                print(
                    f"[UNRESOLVED AD URL END] {visits_number}/{len(unique_unresolved_ad_urls)}\n\n"
                )
                # Save the changes every 10 visits
                if visits_number % 10 == 0:
                    crawl_conn.commit()
            print(
                f"[BROWSER ADS FINISH] Finished crawled data dynamic ads extraction of browser {index + 1}/{len(oba_browser_ids)}..."
            )

    def update_crawling_data_process(self, filter_ads: bool = True):
        """Process the crawling data, setting the dynamic ads and filtering the ads that are known not to be ads."""
        # TODO: Make this process resumable between each new crawling. i.e modify the same crawling_database,
        # creating the tables only if it is a fresh experiment (whose data hasn't been processed yet), and
        # handling the browser_ids marking the ones that have already been processed so after preprocesses,
        # we only update the tables with all the data from the browsers of the new crawlings that haven't been processed yet.

        def read_browser_ids() -> Tuple:
            file_path = Path(
                self.experiment_data_dir + f"{self.experiment_name}_config.json"
            )
            # File exists, load the existing JSON
            with open(file_path, "r") as f:
                experiment_config_json = json.load(f)

            clear_browser_ids = experiment_config_json["browser_ids"]["clear"]
            oba_browser_ids = experiment_config_json["browser_ids"]["oba"]
            return clear_browser_ids, oba_browser_ids

        # Connect to crawling SQLite database
        # sqlite_path = Path(self.experiment_data_dir + "crawl-data.sqlite")
        sqlite_path = Path(self.experiment_data_dir + "crawl-data.sqlite")
        crawl_conn = sqlite3.connect(sqlite_path)
        crawl_cursor = crawl_conn.cursor()

        # Fetch both browser ids (clean run and crawl)
        clear_browser_ids, oba_browser_ids = read_browser_ids()

        self.filter_ads(non_ads=True, unspecific_ads=True)

        # Set dynamic ads for the Instance
        self.process_browsers_new_ads(crawl_cursor, crawl_conn, oba_browser_ids)

        # Save the changes
        crawl_conn.commit()

    def filter_ads(self, non_ads: bool = False, unspecific_ads: bool = False):
        """
        Marks the ADS that have ad_urls known not to be ads as non_ad or unspecific_ad in the database. This is done by matching substrings of the ad_url with the known not ad urls.
        Non ads are urls of ad provider settings, privacy policies, etc.
        Unspecific ads urls are ads whose ad_url was captured as just the domain of a search engine where the ad was displayed as a result, therefore the specific ad is lost.

        """
        # Connect to crawling SQLite database
        sqlite_path = Path(self.experiment_data_dir + "crawl-data.sqlite")
        crawl_conn = sqlite3.connect(sqlite_path)
        crawl_cursor = crawl_conn.cursor()

        if non_ads:
            # Read the known not ad urls
            with open("oba/resources/non_ads_urls.txt", "r") as f:
                not_ad_urls = f.read().splitlines()

            # Mark the known non ad urls as not_ad if the url contains the non ad url
            for not_ad_url in not_ad_urls:
                crawl_cursor.execute(
                    "UPDATE visit_advertisements SET non_ad=1 WHERE ad_url LIKE :ad_url",
                    {"ad_url": f"%{not_ad_url}%"},
                )

            # Save the changes
            crawl_conn.commit()

        if unspecific_ads:
            # Read the known not ad urls
            with open("oba/resources/unspecific_ads_urls.txt", "r") as f:
                unspecific_ads = f.read().splitlines()

            # Mark the known unspecific ad urls as not_ad
            for unspecific_ad_url in unspecific_ads:
                crawl_cursor.execute(
                    "UPDATE visit_advertisements SET unspecific_ad=1 WHERE ad_url LIKE :ad_url",
                    {"ad_url": f"%{unspecific_ad_url}%"},
                )

            # Save the changes
            crawl_conn.commit()

        print("Finished marking ads filter columns")


# style_and_fashion_experiment_accept_data_processer = DataProcesser(
#     "style_and_fashion_experiment_accept", WEBSHRINKER_CREDENTIALS
# )

# # style_and_fashion_experiment_accept_data_processer.update_crawling_data_process()
# style_and_fashion_experiment_accept_data_processer.filter_ads(
#     non_ads=True, unspecific_ads=True
# )

test_experiment = DataProcesser(
    "test_nohup_style_and_fashion_experiment_accept", WEBSHRINKER_CREDENTIALS
)

test_experiment.filter_ads(non_ads=True, unspecific_ads=True)
test_experiment.update_crawling_data_process()
