import json
import sqlite3

import os
import sqlite3
import pandas as pd


import sqlite3
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt
from oba.enums import IAB_CATEGORIES

DATA_FROM_VOLUME = True
DATA_CONTROL_RUNS = False


class ExperimentMetrics:
    def __init__(self, experiment_name: str, control_runs=DATA_CONTROL_RUNS):
        """Initialize the analyzer with the path to the SQLite database."""
        self.control_runs = control_runs
        self.experiment_name = experiment_name
        if DATA_FROM_VOLUME:
            self.experiment_dir = (
                f"/Volumes/LaCie/OpenOBA/oba_runs/{self.experiment_name}"
            )
            self.data_path = f"{self.experiment_dir}/crawl-data-copy.sqlite"
        else:
            self.experiment_dir = f"../datadir/{experiment_name}"
            self.data_path = f"{self.experiment_dir}/crawl-data.sqlite"

        if control_runs:
            self.experiment_dir = (
                f"/Volumes/LaCie/OpenOBA/control_runs/{self.experiment_name}"
            )
            self.data_path = f"{self.experiment_dir}/crawl-data.sqlite"
        self.experiment_config = self._read_experiment_config_json()
        self.oba_browsers = self.experiment_config["browser_ids"]["oba"]
        self.clean_run_browsers = self.experiment_config["browser_ids"]["clear"]
        self.conn = sqlite3.connect(self.data_path)

    def write_results_file_with_data(self, data, file_name, index=False):
        """Write data from a Pandas DataFrame to a file in the specified path."""
        file_path = f"{self.experiment_dir}/results/{file_name}"
        data.to_csv(file_path, index=index)

    def _read_experiment_config_json(self):
        """Opens and reads the json file with the configuration for the experiment"""
        file_path = f"{self.experiment_dir}/{self.experiment_name}_config.json"
        # Check if the file exists
        if os.path.isfile(file_path):
            # File exists, load the existing JSON
            with open(file_path, "r") as file:
                experiment_json = json.load(file)
            return experiment_json
        else:
            raise FileNotFoundError(
                f"Trying to read file in relative path {file_path} which does not"
                " exist."
            )

    def _execute_query(self, query):
        """Execute a given SQL query and return the results."""
        cur = self.conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        return results

    def get_visits_by_url_summary(self):
        """Retrieve a summary of visits."""
        query = """
        SELECT url, COUNT(*) as num_visits
        FROM visits
        GROUP BY url
        ORDER BY url
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["URL", "NumVisits"])

    def get_ads_by_category_table_all_browsers(self) -> List[Dict]:
        """Fetch the number of ads per category for all browsers"""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        query = """
                SELECT 
                    lpc.category_name as category_name,
                    COUNT(CASE WHEN va.browser_id = :session_1_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession1,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_1_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession1,
                    COUNT(CASE WHEN va.browser_id = :session_2_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession2,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_2_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession2,
                    COUNT(CASE WHEN va.browser_id = :session_3_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession3,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_3_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession3,
                    COUNT(CASE WHEN va.browser_id = :session_4_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession4,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_4_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession4,
                    COUNT(CASE WHEN va.browser_id = :session_5_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession5,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_5_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession5,
                    COUNT(CASE WHEN va.browser_id = :session_6_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession6,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_6_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession6,
                    COUNT(CASE WHEN va.browser_id = :session_7_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession7,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_7_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession7,
                    COUNT(CASE WHEN va.browser_id = :session_8_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession8,
                    COUNT(DISTINCT CASE WHEN va.browser_id = :session_8_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession8
                FROM visit_advertisements va
                LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
                LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
                WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
                GROUP BY lpc.category_name
            """
        cursor.execute(
            query,
            {
                "session_1_browser_id": self.oba_browsers[0],
                "session_2_browser_id": self.oba_browsers[1],
                "session_3_browser_id": self.oba_browsers[2],
                "session_4_browser_id": self.oba_browsers[3],
                "session_5_browser_id": self.oba_browsers[4],
                "session_6_browser_id": self.oba_browsers[5],
                "session_7_browser_id": self.oba_browsers[6],
                "session_8_browser_id": self.oba_browsers[7],
            },
        )
        ads = cursor.fetchall()
        cursor.close()

        ads_df = pd.DataFrame(
            ads,
            columns=[
                "Category",
                "NumAdsURLSession1",
                "NumUniqueAdsURLSession1",
                "NumAdsURLSession2",
                "NumUniqueAdsURLSession2",
                "NumAdsURLSession3",
                "NumUniqueAdsURLSession3",
                "NumAdsURLSession4",
                "NumUniqueAdsURLSession4",
                "NumAdsURLSession5",
                "NumUniqueAdsURLSession5",
                "NumAdsURLSession6",
                "NumUniqueAdsURLSession6",
                "NumAdsURLSession7",
                "NumUniqueAdsURLSession7",
                "NumAdsURLSession8",
                "NumUniqueAdsURLSession8",
            ],
        )

        # Add the total number of ads and unique ads for each category
        total_ads = ads_df.filter(like="NumAdsURL").sum(axis=1)
        total_unique_ads = ads_df.filter(like="NumUniqueAdsURL").sum(axis=1)

        # Insert the total number of ads and unique ads at the beginning of the dataframe
        ads_df.insert(1, "TotalNumAdsURL", total_ads)
        ads_df.insert(2, "TotalNumUniqueAdsURL", total_unique_ads)

        # Sort the rows by the total number of unique ads
        ads_df = ads_df.sort_values(by="TotalNumUniqueAdsURL", ascending=False)

        print(ads_df)

        # Add a new column with a boolean indicating if the category is tier 1 or not
        tier1_categories = []
        for key, value in IAB_CATEGORIES.items():
            tier1_categories.append(value[key])

        ads_df.insert(1, "IsTier1", ads_df["Category"].isin(tier1_categories))

        return ads_df

    def get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
        self,
        numvisits_by_browser_id_and_url: List[Tuple[int, str, int]],
    ) -> List[Dict]:
        """Given a list of tuples where the first element is the browser_id, the second is the site_url and the third is the number of visits, return the number of distinct ad_urls and the number of distinct ad_urls whose landing page was categorized with the given category for each session."""

        def fetch_ads_by_limits_and_site_url_and_category(
            cursor,
            site_url: str,
            num_visits: int,
            already_taken: int,
            category_name: str,
        ):
            # print(
            #     f"Fetching ads for {site_url} with {num_visits} visits. Already taken: {already_taken} visits from this site."
            # )

            query = """
                WITH VisitAdCounts AS (
                    SELECT
                        sv.site_rank,
                        COUNT(CASE WHEN lpc.category_name = :category_name THEN va.ad_url ELSE NULL END) AS NumAdsURL,
                        COUNT(DISTINCT CASE WHEN lpc.category_name = :category_name THEN va.ad_url ELSE NULL END) AS NumUniqueAdsURL
					FROM site_visits sv 
                    LEFT JOIN visit_advertisements va ON sv.visit_id = va.visit_id
                    LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
                    LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
                    WHERE sv.site_url = :site_url
                    GROUP BY sv.site_rank
                ),
                OrderedVisits AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY site_rank) AS rownum
                    FROM VisitAdCounts
                )
                SELECT SUM(NumAdsURL), SUM(NumUniqueAdsURL)
                FROM OrderedVisits
                WHERE rownum > :already_taken AND rownum <= :already_taken + :num_visits
                ORDER BY site_rank;
                """

            data = {
                "category_name": category_name,
                "control_run": 1 if self.control_runs else 0,
                "site_url": site_url,
                "num_visits": num_visits,
                "already_taken": already_taken,
            }

            cursor.execute(query, data)

            # print(query, data)

            result = cursor.fetchall()
            return result

        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        # Fetch all the category names found in the database
        query = """
            SELECT DISTINCT category_name
            FROM landing_page_categories
            """
        cursor.execute(query)
        categories = cursor.fetchall()
        categories = [category[0] for category in categories]

        # Initialize the sessions list and the set of seen browser_ids
        site_visits_count = {
            site_url: 0 for _, site_url, _ in numvisits_by_browser_id_and_url
        }

        # Create a dictionary with the number of ads and unique ads for each session for each category
        categories_ads = {}
        for category in categories:
            categories_ads[category] = {}
            for i in range(1, 9):
                categories_ads[category][f"NumAdsURL_Session{i}"] = 0
                categories_ads[category][f"NumUniqueAdsURL_Session{i}"] = 0

        session_number = 0  # Initialize the session number
        seen_browser_ids = set()
        for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
            if browser_id not in seen_browser_ids:
                # Move to the next session
                session_number += 1

                # Add the session to seen sessions
                seen_browser_ids.add(browser_id)

            for category in categories:
                # Fetch the ads from num_visits visits to the current site_url, starting from the first visit after the already taken visit
                query_result = fetch_ads_by_limits_and_site_url_and_category(
                    cursor,
                    site_url,
                    num_visits,
                    site_visits_count[site_url],
                    category,
                )

                # Add the number of ads and unique ads to the corresponding category
                numAdsURL, numUniqueAdsURL = query_result[0]

                key_num_ads_url = f"NumAdsURL_Session{session_number}"
                key_num_unique_ads_url = f"NumUniqueAdsURL_Session{session_number}"

                categories_ads[category][key_num_ads_url] += numAdsURL
                categories_ads[category][key_num_unique_ads_url] += numUniqueAdsURL

            # Update the number of visits taken from the site_url
            site_visits_count[site_url] += num_visits

        cursor.close()

        # With the dictionary of categories_ads, we can now create a DataFrame where each row is a category and each column is the number of ads and unique ads for each session
        df = pd.DataFrame(categories_ads).T

        # Name the index
        df.index.name = "Category"

        # Add the total number of ads and unique ads for each category
        total_ads = df.filter(like="NumAdsURL").sum(axis=1)
        total_unique_ads = df.filter(like="NumUniqueAdsURL").sum(axis=1)

        # Insert the total number of ads and unique ads at the beginning of the dataframe
        df.insert(0, "TotalNumAdsURL", total_ads)
        df.insert(1, "TotalNumUniqueAdsURL", total_unique_ads)

        # Sort the rows by the total number of unique ads
        df = df.sort_values(by="TotalNumUniqueAdsURL", ascending=False)

        print(df)

        # Add a new column with a boolean indicating if the category is tier 1 or not
        tier1_categories = []
        for key, value in IAB_CATEGORIES.items():
            tier1_categories.append(value[key])

        df.insert(0, "IsTier1", df.index.isin(tier1_categories))

        return df

    def get_experiment_summary(self):
        """Retrieve a summary of visits."""

        # First we get the number of control and training visits
        query = """
        SELECT 
            COUNT(CASE WHEN site_rank IS NULL THEN 1 ELSE NULL END) as training_visits,
            COUNT(CASE WHEN site_rank > 0 THEN site_rank ELSE NULL END) as control_visits
        FROM site_visits
        """
        results_site_visits = self._execute_query(query)

        df = pd.DataFrame(
            results_site_visits, columns=["TrainingVisits", "ControlVisits"]
        )

        # Then we get numbers about the ads

        query = """
       SELECT 
            COUNT(DISTINCT ad_url) as num_ads,
            COUNT(DISTINCT CASE WHEN va.non_ad  IS NULL AND va.unspecific_ad IS NULL THEN ad_url ELSE NULL END) as filtered_ads,
			COUNT(DISTINCT CASE WHEN (va.non_ad IS NULL AND va.unspecific_ad IS NULL) AND va.categorized = TRUE AND lpc.category_name != "Uncategorized" THEN va.ad_url ELSE NULL END) as categorized_ads,
			COUNT(DISTINCT CASE WHEN (va.non_ad IS NULL AND va.unspecific_ad IS NULL) AND va.categorized = TRUE AND lpc.category_name != "Uncategorized" AND lpc.confident = 1 THEN va.ad_url ELSE NULL END) as confident_ads
        FROM visit_advertisements va
        LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
        LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
        """
        results_ads = self._execute_query(query)

        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    results_ads,
                    columns=["NumAds", "FilteredAds", "CategorizedAds", "ConfidentAds"],
                ),
            ],
            axis=1,
        )

        return df

    def get_control_visits_by_url_and_browser(self, as_dict=False):
        """Retrieve a summary of visits."""
        query = """
            SELECT browser_id, site_url, COUNT(*) as num_visits
            FROM site_visits
            WHERE site_url IN (
                'http://myforecast.com/',
                'http://weatherbase.com/',
                'http://theweathernetwork.com/',
                'http://weather.com/',
                'http://weather2umbrella.com/'
            )
            GROUP BY browser_id, site_url
            ORDER BY browser_id, site_url;
        """
        results = self._execute_query(query)

        # Filter and sort the results by browser_id that only appear in the OBA browsers
        results = [row for row in results if row[0] in self.oba_browsers]

        results = sorted(results, key=lambda x: self.oba_browsers.index(x[0]))

        return results
        # if as_dict:
        #     results_dict = {}
        #     for browser_id, site_url, num_visits in results:
        #         if browser_id not in results_dict:
        #             results_dict[browser_id] = {}
        #         results_dict[browser_id][site_url] = num_visits
        #     return results_dict

        # return pd.DataFrame(results, columns=["browser_id", "URL", "NumVisits"])

    def get_control_visits_by_browser(self, as_dict=False):
        """Retrieve a summary of visits."""
        query = """
            SELECT browser_id, COUNT(*) as num_visits
            FROM site_visits
            WHERE site_url IN (
                'http://myforecast.com/',
                'http://weatherbase.com/',
                'http://theweathernetwork.com/',
                'http://weather.com/',
                'http://weather2umbrella.com/'
            )
            GROUP BY browser_id;
        """
        results = self._execute_query(query)
        if as_dict:
            return {row[0]: {row[1]: row[2]} for row in results}
        else:
            return pd.DataFrame(results, columns=["browser_id", "NumVisits"])

    def get_ads_summary(self):
        """Retrieve a summary of advertisements."""
        query = """
        SELECT visit_url, COUNT(DISTINCT ad_url) as num_ads
        FROM visit_advertisements
        WHERE categorized = TRUE AND non_ad IS NULL AND unspecific_ad IS NULL
        GROUP BY visit_url
        ORDER BY visit_url;
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["Visit URL", "NumAds"])

    def get_landing_pages_summary(self):
        """Retrieve a summary of landing pages and their categorization status."""
        query = """
        SELECT categorized, COUNT(*) as num_landing_pages
        FROM landing_pages
        GROUP BY categorized
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["Categorized", "NumLandingPages"])

    def get_category_distribution(self):
        """Retrieve the distribution of landing page categories."""
        query = """
        SELECT category_name, COUNT(*) as count
        FROM landing_page_categories
        GROUP BY category_name
        ORDER BY count DESC
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["CategoryName", "Count"])

    def close(self):
        """Close the database connection."""
        self.conn.close()

    # Placeholder for more complex analysis methods
    # def some_complex_analysis(self):
    #     pass


# Usage example:
# analyzer = ExperimentMetrics("style_and_fashion_experiment_accept")
# print(analyzer.get_visits_summary())
# print(analyzer.get_ads_summary())
# print(analyzer.get_landing_pages_summary())
# print(analyzer.get_category_distribution())
# analyzer.close()


# Usage example
# analyzer = OBAAnalyzer("style_and_fashion_experiment_accept")
# analyzer.analyze_data()
