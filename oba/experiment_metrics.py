import json
import sqlite3

import os
import sqlite3
import pandas as pd


import sqlite3
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt

DATA_FROM_VOLUME = True
DATA_CONTROL_RUNS = False


class ExperimentMetrics:
    def __init__(self, experiment_name: str, control_runs=DATA_CONTROL_RUNS):
        """Initialize the analyzer with the path to the SQLite database."""
        self.control_runs = control_runs
        self.experiment_name = experiment_name
        if DATA_FROM_VOLUME:
            self.experiment_dir = (
                f"/Volumes/FOBAM_data/8_days/datadir/{self.experiment_name}"
            )
            self.data_path = f"{self.experiment_dir}/crawl-data-copy.sqlite"
        else:
            self.experiment_dir = f"../datadir/{experiment_name}"
            self.data_path = f"{self.experiment_dir}/crawl-data.sqlite"

        if control_runs:
            self.experiment_dir = (
                f"/Volumes/FOBAM_data/control_runs/{self.experiment_name}"
            )
            self.data_path = f"{self.experiment_dir}/crawl-data.sqlite"
        self.experiment_config = self._read_experiment_config_json()
        self.oba_browsers = self.experiment_config["browser_ids"]["oba"]
        self.clean_run_browsers = self.experiment_config["browser_ids"]["clear"]
        self.conn = sqlite3.connect(self.data_path)

    def write_results_file_with_data(self, data, file_name):
        """Write data from a Pandas DataFrame to a file in the specified path."""
        file_path = f"{self.experiment_dir}/results/{file_name}"
        data.to_csv(file_path, index=False)

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
                    COUNT(va.ad_url) as ad_urls,
                    COUNT(DISTINCT va.ad_url) as unique_ad_urls
                FROM visit_advertisements va
                LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
                LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
                WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
                GROUP BY lpc.category_name
                ORDER BY COUNT(DISTINCT va.ad_url) DESC
            """
        cursor.execute(query)
        ads = cursor.fetchall()
        cursor.close()

        ads_df = pd.DataFrame(ads, columns=["Category", "NumAds", "UniqueAds"])
        return ads_df

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
