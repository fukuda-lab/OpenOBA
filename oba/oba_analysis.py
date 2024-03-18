import json
import sqlite3

import os
import sqlite3
import pandas as pd


import sqlite3
from typing import List, Dict, Literal, Tuple
import matplotlib.pyplot as plt

DATA_FROM_VOLUME = True


class OBAQuantifier:
    def __init__(self, experiment_name: str, experiment_category: str):
        """Initialize the analyzer with the path to the SQLite database."""
        self.experiment_name = experiment_name
        # self.data_path = f"../datadir/{experiment_name}/crawl-data.sqlite"
        if DATA_FROM_VOLUME:
            # self.experiment_data_dir = (
            #     f"/Volumes/FOBAM_data/8_days/datadir/{self.experiment_name}/"
            # )
            # self.db_path = f"{self.experiment_data_dir}/crawl-data-copy.sqlite"
            self.experiment_data_dir = (
                f"/Volumes/FOBAM_data/control_runs/{self.experiment_name}/"
            )
            self.db_path = f"{self.experiment_data_dir}crawl-data.sqlite"
        else:
            self.experiment_data_dir = f"datadir/{self.experiment_name}/"
            self.db_path = f"{self.experiment_data_dir}/crawl-data.sqlite"

        os.makedirs(f"{self.experiment_data_dir}/results", exist_ok=True)
        self.results_dir = f"{self.experiment_data_dir}/results"

        self.experiment_config = self._read_experiment_config_json()

        self.oba_browsers_ids = self.experiment_config["browser_ids"]["oba"]
        self.clean_run_browsers_ids = self.experiment_config["browser_ids"]["clear"]

        self.output_path = (
            f"datadir/{self.experiment_name}/results/chronological_progress.json"
        )
        self.experiment_category = experiment_category
        self.conn = None

    def connect(self):
        """Establish a connection to the SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Access columns by name

    def disconnect(self):
        """Close the connection to the SQLite database."""
        if self.conn:
            self.conn.close()

    def _read_experiment_config_json(self):
        """Opens and reads the json file with the configuration for the experiment"""
        file_path = self.experiment_data_dir + f"{self.experiment_name}_config.json"
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

    def fetch_ads_by_browser_id_and_category(
        self, browser_id: int, category: str = None
    ) -> List[Dict]:
        """Given a specific brower_id (i.e. session). From all the categorized advertisements, return the number of distinct ad_urls and the number of ad_urls whose landing page was categorized with the given category."""
        if category is None:
            category = self.experiment_category

        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        query = """
                SELECT 
                    COUNT(DISTINCT va.ad_url) as distinct_ad_urls,
                    COUNT(DISTINCT CASE WHEN lpc.category_name = :category_name THEN va.ad_url END) as ad_urls_in_category
                FROM visit_advertisements va
                LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
                LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
                WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND va.browser_id = :browser_id AND va.clear_run = FALSE
            """

        cursor.execute(
            query,
            {
                "browser_id": browser_id,
                "category_name": category,
            },
        )
        ads = cursor.fetchall()
        cursor.close()
        return ads

    def fetch_all_ads_grouped_by_browser_id(self, category: str = None) -> List[Dict]:
        """Given a specific brower_id (i.e. session). From all the categorized advertisements, return the number of distinct ad_urls and the number of ad_urls whose landing page was categorized with the given category."""
        if category is None:
            category = self.experiment_category

        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        query = """
                SELECT 
                    va.browser_id,
                    COUNT(DISTINCT va.ad_url) as distinct_ad_urls,
                    COUNT(DISTINCT CASE WHEN lpc.category_name = :category_name THEN va.ad_url END) as ad_urls_in_category
                FROM visit_advertisements va
                LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
                LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
                WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND va.clean_run = TRUE AND lpc.category_name != "Uncategorized"
                GROUP BY va.browser_id
            """

        cursor.execute(
            query,
            {
                "category_name": category,
            },
        )
        result = cursor.fetchall()

        # Convert to a list of dictionaries
        result = [dict(row) for row in result]

        sorted_result = []

        for browser_id in self.clean_run_browsers_ids:
            for row in result:
                if row["browser_id"] == browser_id:
                    sorted_result.append(row)
                    break

        cursor.close()
        return sorted_result

    def fetch_all_ads_grouped_by_artificial_session(
        self, visits_url_list_ordered: List[str], category: str = None
    ) -> List[Dict]:
        """Given a list of URLS, simulate different sessions mimicking the behavior of different browser_ids and return the number of distinct ad_urls and the number of ad_urls whose landing page was categorized with the given category. The order of the list will be the order of the simulated sessions.
        Bear in mind that the database only has one browser_id, so the simulated sessions will not
        """

    def plot_ads_by_browser_id(
        self,
        ads_by_browser_id: List[Dict],
        cookie_banner_option: Literal[0, 1, 2],
        file_name_suffix: str = "",
    ):
        """Given a list of dictionaries with the number of ads in a category and the total number of ads, plot grouped bars and save to file."""
        # Convert to a pandas DataFrame
        df = pd.DataFrame(ads_by_browser_id)

        # Setting the positions and width for the bars
        positions = list(range(len(df)))
        width = 0.35  # the width of the bars

        # Plotting
        fig, ax = plt.subplots()
        plt.bar(
            [p - width / 2 for p in positions],
            df["ad_urls_in_category"],
            width,
            alpha=0.5,
            label="Style and Fashion",
        )
        plt.bar(
            [p + width / 2 for p in positions],
            df["distinct_ad_urls"],
            width,
            alpha=0.5,
            label="Any Category",
        )

        # Customizing the plot
        cookie_banner_title = ["Ignore", "Accept", "Reject"][cookie_banner_option]
        ax.set_ylabel("Number of Ads")
        ax.set_title(
            f"Ads Analysis per Session with Cookie Banner: {cookie_banner_title}"
        )
        ax.set_xticks(positions)
        ax.set_xticklabels(df["browser_id"], rotation=45, ha="right")
        ax.legend()
        plt.tight_layout()

        ax.set_ylim(0, 210)

        # Save the plot to a file
        plot_filename = f"{self.results_dir}/ads_analysis_{cookie_banner_title}_{file_name_suffix}.png"
        plt.savefig(plot_filename)
        print(f"Plot saved to {plot_filename}")

        plt.close(fig)  # Close the figure to free memory
