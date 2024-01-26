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

import aiohttp
import tldextract
from adblockparser import AdblockRules
from categorizer import Categorizer
from enums import (
    AdvertisementsCategoriesQueries,
    AdvertisementsQueries,
    CleanBrowserQueries,
    ControlVisitAdsQueries,
    ControlVisitsQueries,
    CrawlDataQueries,
    GenericQueries,
    OBABrowserQueries,
)
from extract_ad_url import process

credentials = {"api_key": "GhU39K7bdfvdxRlcnEkT", "secret_key": "ZwnCzHIpw08DF10Fmz5c"}


class DataProcesser:
    # Create a logger
    logger = logging.getLogger(__name__)

    def __init__(self, experiment_name: str, webshrinker_credentials):
        self.experiment_name = experiment_name
        if webshrinker_credentials:
            self.categorizer = Categorizer(**webshrinker_credentials)

        # [OLD EXPERIMENTS]
        # self.source_pages_dir = f'../datadir/{self.experiment_name}/sources/{self.experiment_name}'
        # self.experiment_data_path = f'../datadir/{self.experiment_name}.sqlite'
        # self.ads_database = f'results/{self.experiment_name}/ads_db.sqlite'
        # self.output_path = f'results/{self.experiment_name}/chronological_progress.json'

        # Dirs, maybe would be better in a dictionary Paths
        self.source_pages_dir = f"../datadir/{self.experiment_name}/sources"
        self.experiment_data_path = (
            f"../datadir/{self.experiment_name}/crawl-data.sqlite"
        )

        os.makedirs(f"datadir/{self.experiment_name}/results", exist_ok=True)
        self.output_path = (
            f"datadir/{self.experiment_name}/results/chronological_progress.json"
        )
        self.ads_database = f"datadir/{self.experiment_name}/results/ads_db.sqlite"

        self.static_ads = []
        self.dynamic_ads = defaultdict(list)
        self.control_site_urls = set()
        self.blocklists_paths_list = ["easylist.txt", "easyprivacy.txt"]

    # def categorize_ads(self, webshrinker_credentials):
    #     """ with the Categorizer, Categorize all the adds captured from the control pages visits (including the clean browser ads) """

    #     # Connect to the same db from the training_pages or handle its own?

    #     pass

    def _get_site_ads_from_visit(self, visit_id: AnyStr) -> Tuple[List, List]:
        """Use the extract_ad_url script to get the ads of a page source given its visit_id."""
        # TODO: I don't remember very well why this, check it well
        # Not sure about this way of doing it
        static_ads_here = set() if not self.static_ads else self.static_ads
        # Unpack row
        # Search for the JSON file using a pattern
        pattern = os.path.join(
            self.source_pages_dir, f"{visit_id}-*.json.gz"
        )  # Match all the ads
        json_files = glob.glob(pattern)

        href_urls = []
        landing_page_urls = []
        # Process each file that matches the pattern
        for json_filename in json_files:
            ad_url_data = process(self.blocklists_paths_list, json_filename)
            for _, value in ad_url_data.items():
                if value["landing_page_url"] not in static_ads_here:
                    landing_page_urls.append(value["landing_page_url"])
                    href_urls.append(value["href_url"])
        return (landing_page_urls, href_urls)

    def _create_ad_tables_if_not_exist(self, ads_cursor):
        """Creates the database for the control pages visited during the crawling related data"""

        # Table for site_visits during the crawling that correspond to control pages
        ads_cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS ControlVisits (
                    id INTEGER PRIMARY KEY,
                    visit_id INTEGER UNIQUE,
                    browser_id INTEGER,
                    site_url TEXT,
                    site_rank INTEGER
                    )
            """
        )

        # Table for standalone Advertisements. Static is boolean so represented with 0 or 1
        ads_cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS Advertisements (
                    id INTEGER PRIMARY KEY,
                    landing_url TEXT UNIQUE,
                    static INTEGER
                )
            """
        )

        # Table that standardizes the several Advertisements that could have appeared for each entry of ControlVisits
        # The ad_url would be the
        ads_cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS ControlVisitAds (
                    id INTEGER PRIMARY KEY,
                    control_visit_id INTEGER,
                    control_site_url TEXT,
                    control_site_rank INTEGER,
                    ad_id INTEGER,
                    landing_url TEXT,
                    ad_href_url TEXT,
                    FOREIGN KEY(control_visit_id) REFERENCES ControlVisits(id),
                    FOREIGN KEY(ad_id) REFERENCES Advertisements(id)
                )
            """
        )

        # Table that standardizes the several categories that an Advertisement can have
        ads_cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS AdvertisementsCategories (
                    id INTEGER PRIMARY KEY,
                    ad_id INTEGER,
                    landing_url TEXT,
                    category TEXT,
                    taxonomy TEXT,
                    taxonomy_tier INTEGER,
                    taxonomy_id TEXT,
                    confident BOOLEAN,
                    FOREIGN KEY(ad_id) REFERENCES Advertisements(id)
                )
            """
        )

    def _set_and_save_static_ads_and_control_sites(
        self, crawl_cursor, ads_cursor, clear_browser_ids
    ):
        """Get and set the static ads from the clear browsers"""

        def _get_static_ads_for_visit(clean_visit_rows: List) -> Set:
            """Static ads use a set because we don't care about duplicates and we don't need site_rank"""

            static_ads_new_set = set()
            for clean_row in clean_visit_rows:
                _, visit_id = clean_row
                ads_tuple = self._get_site_ads_from_visit(visit_id)
                static_ads_new_set = static_ads_new_set | set(ads_tuple[0])
            return static_ads_new_set

        static_ads_set = set()

        # Get static ads
        for index, browser_id in enumerate(clear_browser_ids):
            print(
                f"Processing static ads of browser {index + 1}/{len(clear_browser_ids)}"
            )
            clear_browser_queries = CleanBrowserQueries(browser_id=browser_id)
            # Get all the static and contextual ads (clean browser)
            crawl_cursor.execute(clear_browser_queries.CleanRunVisitsQuery)
            clean_run_site_visits = crawl_cursor.fetchall()
            static_ads_set = static_ads_set | _get_static_ads_for_visit(
                clean_run_site_visits
            )

            # Get all the control_sites urls in a list
            # TODO: Check why is that if like that (shouldn't it be just if clean_run_site_visits?)
            if len(clean_run_site_visits) > len(self.control_site_urls):
                for control_site_url, _ in clean_run_site_visits:
                    self.control_site_urls.add(control_site_url)
        self.static_ads = list(static_ads_set)

        # TODO: Check if the static ads are being correclty saved, the return values with the tuple of lists and stuff
        for static_ad in self.static_ads:
            # We set static as True by using int "1" because of how sqlite handles booleans
            try:
                ads_cursor.execute(AdvertisementsQueries.InsertAdQuery, (static_ad, 1))
            except sqlite3.IntegrityError:
                # Duplicate, pass without raising error
                pass

    def _get_javascripts_third_party_urls(
        self, crawl_cursor, browser_id, visit_id, site_url
    ):
        """Gets third party urls found within javascripts run given a visit_id. This are potential ads"""

        def find_urls_in_js(js_string, site_url_tld):
            """Regex to find URLs in a string (for example) ['www.example.com', 'api.test.com', 'array1.com', 'array2.com', 'airfrance.fr', 'www.google.com']"""
            # Regular expression to match URLs
            url_pattern = re.compile(
                r'\b(https?://)?(?:www\.)?([a-z0-9-]+(?:\.[a-z0-9-]+)+)(?=[\'";\s])'
            )
            matches = set()
            # Get all matches
            for match_found in url_pattern.findall(js_string):
                # Must be third parties
                if tldextract.extract(match_found[1]).domain != site_url_tld:
                    matches.add(match_found[0] + match_found[1])
            return matches

        # 2. With browser_id and visit_id, retrieve all the javascripts captured in that visit
        crawl_cursor.execute(
            CrawlDataQueries.SelectJavascriptsQuery, (browser_id, visit_id)
        )
        javascripts = crawl_cursor.fetchall()

        # 3. Look within the javascripts for all the third party URLs
        urls_found = set()
        site_url_tld = tldextract.extract(site_url).domain
        for javascript in javascripts:
            (
                js_id,
                frame_id,
                script_url,
                document_url,
                top_level_url,
                func_name,
                call_stack,
                symbol,
                operation,
                value,
                arguments,
                time_stamp,
            ) = javascript
            urls_in_value = find_urls_in_js(
                value, top_level_url, script_url, site_url_tld
            )
            urls_found |= urls_in_value
            # urls_in_call_stack = find_urls_in_js(call_stack)
            # urls_in_symbol = find_urls_in_js(symbol)
            # urls_in_arguments = find_urls_in_js(arguments)

        # 4. BLocklist to get ads, mark them as href when matched
        # Open blocklists
        filter_l_l = [
            open(path).read().splitlines() for path in self.blocklists_paths_list
        ]
        # Flatten them
        filter_l = [item for sublist in filter_l_l for item in sublist]
        ad_block = AdblockRules(filter_l)

        possible_hrefs = []
        possible_landing_pages = []

        for url in urls_found:
            if ad_block.should_block(url):
                possible_hrefs.append(url)
            else:
                possible_landing_pages.append(url)

        return possible_hrefs, possible_landing_pages

    def _set_and_save_dynamic_ads(
        self,
        crawl_cursor,
        ads_cursor,
        oba_browser_ids,
        request_rate=1,
        taxonomy="iabv1",
    ):
        """Set dynamic ads, in a dictionary with all the ads by control_site and visit_id ordered by site_rank"""

        semaphore = asyncio.Semaphore(request_rate)

        async def _async_categorize_ad(landing_ad_url):
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    response = await self.categorizer.categorize(
                        session, landing_ad_url, taxonomy=taxonomy
                    )
                    delay = 1  # Initial delay in seconds
                    while True:
                        if response["status_code"] == 200:
                            # TODO: check case where we would need more than one "top_level_category"
                            for result in response["categories_response"]:
                                ads_cursor.execute(
                                    AdvertisementsQueries.SelectAdIdQuery,
                                    (landing_ad_url,),
                                )  # Single comma for single-element tuple
                                page_id = ads_cursor.fetchone()[0]
                                ads_cursor.execute(
                                    AdvertisementsCategoriesQueries.InsertAdCategoryQuery,
                                    (
                                        page_id,
                                        landing_ad_url,
                                        result["category"],
                                        result["taxonomy"],
                                        result["taxonomy_tier"],
                                        result["taxonomy_id"],
                                        result["confident"],
                                    ),
                                )

                            # TODO: Implement progress bar
                            # progress.update(1)
                            print(f"[SUCCESS] Fetched categories for {landing_ad_url}")
                            break
                        elif response["status_code"] == 429:
                            print(
                                f"[WARNING] Rate limit exceeded for {landing_ad_url}. Retrying in {delay} seconds..."
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
        for index, browser_id in enumerate(oba_browser_ids):
            oba_browser_queries = OBABrowserQueries(browser_id=browser_id)
            # Start with the crawled data ads extraction
            print(
                f"Starting crawled data dynamic ads extraction of browser {index + 1}/{len(oba_browser_ids)}..."
            )
            for control_site_url in self.control_site_urls:
                print(f"[CONTROL_SITE] {control_site_url}")
                # We assume that each control site was visited only once during clean run.
                query = oba_browser_queries.get_visit_rows_per_control_site_query(
                    control_site_url
                )
                # Fetch all the visits associated to the actual control site
                crawl_cursor.execute(query)
                control_site_visit_rows = crawl_cursor.fetchall()
                for visit_row in control_site_visit_rows:
                    site_url, visit_id, site_rank = visit_row
                    visits_number += 1
                    print(f"[VISIT START] {visits_number}")
                    # SQLITE
                    ads_cursor.execute(
                        ControlVisitsQueries.InsertControlVisit,
                        (visit_id, browser_id, site_url, site_rank),
                    )
                    ads_found_page_source = self._get_site_ads_from_visit(visit_id)
                    (
                        possible_ads_js,
                        possible_landing_pages_js,
                    ) = self._get_javascripts_third_party_urls(
                        crawl_cursor, browser_id, visit_id, site_url
                    )

                    # TODO: Fix so the next line can be possible (return value of self._get_site_ads_from_visit has different shape of return value of self._get_javascripts_third_party_urls)
                    # ads_found = ads_found_page_source + possible_ads_js
                    if ads_found:
                        print(f"[ADS FOUND] {len(ads_found)}")
                        # Add href_urls
                        # output_dict['dynamic_ads'][site_url].append({site_rank: {'landing_page_urls': ads_found[0], 'href_urls': ads_found[1]}})
                        # Add only landing_page_urls
                        self.dynamic_ads[site_url].append({site_rank: ads_found[0]})
                        for i in range(len(ads_found[0])):
                            landing_ad_url = ads_found[0][i]
                            href_ad_url = ads_found[1][i]
                            # SQLITE
                            try:
                                ads_cursor.execute(
                                    AdvertisementsQueries.InsertAdQuery,
                                    (landing_ad_url, 0),
                                )
                                ads_cursor.execute(
                                    AdvertisementsQueries.SelectAdIdQuery,
                                    (landing_ad_url,),
                                )
                                ad_id = ads_cursor.fetchone()[0]
                                print(ad_id)
                                # HERE IS WHERE I WANT TO USE THE CATEGORIZE METHOD:
                                # categories = self.categorizer.categorize()
                                # ADD THE CATEGORIES TO THE AdvertisementsCategories table
                                asyncio.run(_async_categorize_ad(landing_ad_url))
                            except sqlite3.IntegrityError:
                                # Duplicate, pass without raising error
                                pass

                            ads_cursor = ads_cursor.execute(
                                AdvertisementsQueries.SelectAdIdQuery, (landing_ad_url,)
                            )  # Single comma for single-element tuple
                            # TODO: Check here not sure about how does it return
                            # fetchall() returns a list of one element in this case, so [0] for the first element, then [0] to get the (only) column queried
                            ad_id = ads_cursor.fetchall()[0][0]
                            # SQLITE
                            ads_cursor.execute(
                                ControlVisitAdsQueries.InsertControlVisitAdQuery,
                                (
                                    visit_id,
                                    control_site_url,
                                    site_rank,
                                    ad_id,
                                    landing_ad_url,
                                    href_ad_url,
                                ),
                            )

    def crawling_data_process(self):
        # TODO: Make this process resumable between each new crawling. i.e modify the same crawling_database,
        # creating the tables only if it is a fresh experiment (whose data hasn't been processed yet), and
        # handling the browser_ids marking the ones that have already been processed so after preprocesses,
        # we only update the tables with all the data from the browsers of the new crawlings that haven't been processed yet.

        def read_browser_ids() -> Tuple:
            file_path = f"experiments/{self.experiment_name}_config.json"
            # File exists, load the existing JSON
            with open(file_path, "r") as f:
                experiment_config_json = json.load(f)

            clear_browser_ids = experiment_config_json["browser_ids"]["clear"]
            oba_browser_ids = experiment_config_json["browser_ids"]["oba"]
            return clear_browser_ids, oba_browser_ids

        # Connect to crawling SQLite database
        crawl_conn = sqlite3.connect(Path(self.experiment_data_path))
        crawl_cursor = crawl_conn.cursor()

        # Create tables
        ads_conn = sqlite3.connect(Path(self.ads_database))
        ads_cursor = ads_conn.cursor()
        self._create_ad_tables_if_not_exist(ads_cursor)

        # Fetch both browser ids (clean run and crawl)
        clear_browser_ids, oba_browser_ids = read_browser_ids()

        # Set static ads for the Instance
        self._set_and_save_static_ads_and_control_sites(
            crawl_cursor, ads_cursor, clear_browser_ids
        )

        # Set dynamic ads for the Instance
        self._set_and_save_dynamic_ads(crawl_cursor, ads_cursor, oba_browser_ids)

        crawl_conn.commit()
        ads_conn.commit()

        # Close the connection to the SQLite database
        crawl_conn.close()
        ads_conn.close()

        # Write output_data to output JSON file
        with open(self.output_path, "w") as output_file:
            output_data = {
                "static_ads": self.static_ads,
                "dynamic_ads": self.dynamic_ads,
            }
            json.dump(output_data, output_file, indent=2)


ads_handler = DataProcesser(
    "airtravel_cookie_banner_yes", webshrinker_credentials=credentials
)
ads_handler.crawling_data_process()
