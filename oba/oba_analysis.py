import json
import sqlite3

import os
import sqlite3
import pandas as pd


import sqlite3
from typing import List, Dict, Literal, Tuple
import matplotlib.pyplot as plt

DATA_FROM_VOLUME = False
DATA_CONTROL_RUNS = False
RESULTS_DIR = "/Volumes/LaCie/OpenOBA/RESULTS"


class OBAAnalysis:
    def __init__(
        self,
        experiment_name: str,
        experiment_category: str,
        control_runs=DATA_CONTROL_RUNS,
    ):
        """Initialize the analyzer with the path to the SQLite database."""
        self.experiment_name = experiment_name
        # self.data_path = f"../datadir/{experiment_name}/crawl-data.sqlite"
        self.control_runs = control_runs
        if DATA_FROM_VOLUME:
            # self.experiment_data_dir = (
            #     f"/Volumes/FOBAM_data/8_days/datadir/{self.experiment_name}"
            # )
            self.experiment_data_dir = (
                f"/Volumes/LaCie/OpenOBA/oba_runs/{self.experiment_name}"
            )
            self.db_path = f"{self.experiment_data_dir}/crawl-data-copy.sqlite"
        else:
            self.experiment_data_dir = f"datadir/{self.experiment_name}"
            self.db_path = f"{self.experiment_data_dir}/crawl-data.sqlite"
        if control_runs:
            self.experiment_data_dir = (
                f"/Volumes/LaCie/OpenOBA/control_runs/{self.experiment_name}"
            )
            self.db_path = f"{self.experiment_data_dir}/crawl-data.sqlite"

        print(f"Using data from {self.db_path}")

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
        file_path = f"{self.experiment_data_dir}/{self.experiment_name}_config.json"
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
                    COUNT(DISTINCT va.ad_url) as unique_ad_urls,
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

    def fetch_all_ads_by_browser_id_as_dict(self, category: str = None) -> List[Dict]:
        """Given a specific brower_id (i.e. session). From all the categorized advertisements, return the number of distinct ad_urls and the number of ad_urls whose landing page was categorized with the given category."""
        if category is None:
            category = self.experiment_category

        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        # query = """
        #         SELECT
        #             va.browser_id,
        #             COUNT(va.ad_url) - COUNT(CASE WHEN lpc.category_name = :category_name THEN va.ad_url END) as NumAdsURL,
        #             COUNT(CASE WHEN lpc.category_name = :category_name THEN va.ad_url END) as NumAdsURLCategory,
        #             COUNT(DISTINCT va.ad_url) - COUNT(DISTINCT CASE WHEN lpc.category_name = :category_name THEN va.ad_url END) as NumUniqueAdsURL,
        #             COUNT(DISTINCT CASE WHEN lpc.category_name = :category_name THEN va.ad_url END) as NumUniqueAdsURLCategory
        #         FROM visit_advertisements va
        #         LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
        #         LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
        #         WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND va.clean_run = :control_run AND lpc.category_name != "Uncategorized"
        #         GROUP BY va.browser_id
        #     """
        query = """
                SELECT 
                    va.browser_id,
                    COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name != "Uncategorized" AND lpc.category_name != :category_name THEN va.ad_id ELSE NULL END) AS NumAdsURL,
                    COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name = :category_name THEN va.ad_id ELSE NULL END) AS NumAdsURLCategory,
                    COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name != "Uncategorized" AND lpc.category_name != :category_name THEN va.ad_url ELSE NULL END) AS NumUniqueAdsURL,
                    COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name = :category_name THEN va.ad_url ELSE NULL END) AS NumUniqueAdsURLCategory
                FROM visit_advertisements va
                LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
                LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
                WHERE va.clean_run = :control_run
                GROUP BY va.browser_id
            """

        cursor.execute(
            query,
            {
                "category_name": category,
                "control_run": 1 if self.control_runs else 0,
            },
        )
        result = cursor.fetchall()

        # Convert to a list of dictionaries
        result = [dict(row) for row in result]

        sorted_result = []

        browsers_ids = (
            self.clean_run_browsers_ids if self.control_runs else self.oba_browsers_ids
        )

        for browser_id in browsers_ids:
            for row in result:
                if row["browser_id"] == browser_id:
                    sorted_result.append(row)
                    break
            if len(sorted_result) == 6:
                break

        cursor.close()
        return sorted_result

    def fetch_all_ads_grouped_by_artificial_sessions_and_site_url(
        self,
        numvisits_by_browser_id_and_url: List[Tuple[int, str, int]],
    ) -> List[Dict]:
        """Given a list of tuples where the first element is the browser_id, the second is the site_url and the third is the number of visits, return the number of distinct ad_urls and the number of distinct ad_urls whose landing page was categorized with the given category for each session."""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        def fetch_ads_by_limits_and_site_url(
            cursor, site_url: str, num_visits: int, already_taken: int
        ):
            # print(
            #     f"Fetching ads for {site_url} with {num_visits} visits. Already taken: {already_taken} visits from this site."
            # )

            query = """
                WITH VisitAdCounts AS (
                    SELECT
                        sv.site_rank,
                        COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name != "Uncategorized" AND lpc.category_name != :category_name THEN va.ad_id ELSE NULL END) AS NumAdsURL,
                        COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name = :category_name THEN va.ad_id ELSE NULL END) AS NumAdsURLCategory,
                        COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name != "Uncategorized" AND lpc.category_name != :category_name THEN va.ad_url ELSE NULL END) AS NumUniqueAdsURL,
                        COUNT(DISTINCT CASE WHEN va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND lpc.category_name = :category_name THEN va.ad_url ELSE NULL END) AS NumUniqueAdsURLCategory
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
                SELECT SUM(NumAdsURL), SUM(NumAdsURLCategory), SUM(NumUniqueAdsURL), SUM(NumUniqueAdsURLCategory)
                FROM OrderedVisits
                WHERE rownum > :already_taken AND rownum <= :already_taken + :num_visits
                ORDER BY site_rank;
                """

            data = {
                "category_name": self.experiment_category,
                "control_run": 1 if self.control_runs else 0,
                "site_url": site_url,
                "num_visits": num_visits,
                "already_taken": already_taken,
            }

            cursor.execute(query, data)

            # print(query, data)

            result = cursor.fetchall()
            return result

        seen_browser_ids = set()
        sessions = []
        site_visits_count = {
            site_url: 0 for _, site_url, _ in numvisits_by_browser_id_and_url
        }

        for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:

            if browser_id not in seen_browser_ids:
                # print("\n" * 2)
                # print((browser_id))
                # Create a new session
                session = {
                    "browser_id": len(sessions) + 1,
                    "NumAdsURL": 0,
                    "NumAdsURLCategory": 0,
                    "NumUniqueAdsURL": 0,
                    "NumUniqueAdsURLCategory": 0,
                }
                sessions.append(session)
                seen_browser_ids.add(browser_id)
            else:
                session = sessions[-1]

            if session["browser_id"] == 7:
                break

            # print("Session: ", session)
            # print("Site URL: ", site_url)
            # print("Visits to check: ", num_visits)
            # print("Site visits: ", site_visits_count)

            # Fetch the ads from num_visits visits to the current site_url, starting from the first visit after the already taken visit
            query_result = fetch_ads_by_limits_and_site_url(
                cursor, site_url, num_visits, site_visits_count[site_url]
            )

            # Update the number of visits taken from the site_url
            site_visits_count[site_url] += num_visits

            # If no ads were found, continue to the next session
            if not query_result or not query_result[0][0]:
                continue

            # Sum the number of distinct ad_urls and the number of ad_urls in the category
            (
                new_NumAdsURL,
                new_NumAdsURLCategory,
                new_NumUniqueAdsURL,
                new_NumUniqueAdsURLCategory,
            ) = query_result[0]

            # Add the new ads to the actual session
            session["NumAdsURL"] += new_NumAdsURL
            session["NumAdsURLCategory"] += new_NumAdsURLCategory
            session["NumUniqueAdsURL"] += new_NumUniqueAdsURL
            session["NumUniqueAdsURLCategory"] += new_NumUniqueAdsURLCategory

        cursor.close()
        return sessions

    def plot_ads_by_browser_id(
        self,
        ads_by_browser_id: List[Dict],
        cookie_banner_option: Literal[0, 1, 2],
        file_name_suffix: str = "",
    ):
        """Given a list of dictionaries with the number of ads in a category and the total number of ads, plot grouped bars and save to file."""
        # Convert to a pandas DataFrame
        df = pd.DataFrame(ads_by_browser_id)

        # Change browser_id to integers from 1 to N conserving the order
        df["browser_id"] = range(1, len(df) + 1)

        print(df)

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
            df["unique_ad_urls"],
            width,
            alpha=0.5,
            label="Any Category",
        )

        # Customizing the plot
        cookie_banner_option_string = ["Ignore", "Accept", "Reject"][
            cookie_banner_option
        ]
        ax.set_ylabel("Number of Ads")

        title = f"Ads per Session with Cookie Banner: {cookie_banner_option_string}"
        if self.control_runs:
            title += " (CONTROL RUNS)"

        ax.set_title(title)
        ax.set_xticks(positions)
        ax.set_xticklabels(df["browser_id"], rotation=45, ha="right")
        ax.legend()
        plt.tight_layout()

        ax.set_ylim(0, 210)

        if self.control_runs:
            file_name_suffix = "CONTROL_" + file_name_suffix

        # Save the plot to a file
        plot_filename = f"{self.results_dir}/ads_analysis_{cookie_banner_option_string}_{file_name_suffix}.png"
        plt.savefig(plot_filename)
        print(f"Plot saved to {plot_filename}")

        # Write the data to a Markdown table file
        markdown_filename = f"{self.results_dir}/ads_analysis_{cookie_banner_option_string}_{file_name_suffix}.md"
        with open(markdown_filename, "w") as file:
            file.write(df.to_markdown())

        print(f"Data saved to {markdown_filename}")
        if RESULTS_DIR:
            # write figure and table to RESULTS_DIR as well
            plot_filename = f"{RESULTS_DIR}/ads_analysis_{cookie_banner_option_string}_{file_name_suffix}.png"
            plt.savefig(plot_filename)
            print(f"Plot saved to {plot_filename}")

            markdown_filename = f"{RESULTS_DIR}/ads_analysis_{cookie_banner_option_string}_{file_name_suffix}.md"
            with open(markdown_filename, "w") as file:
                file.write(df.to_markdown())
            print(f"Data saved to {markdown_filename}")

        plt.close(fig)  # Close the figure to free memory

    def generate_tables_by_session(
        self,
        ads_by_browser_id: List[Dict],
        cookie_banner_option: Literal[0, 1, 2],
        file_name_suffix: str = "",
    ):
        """Given a list of dictionaries with the number of ads in a category and the total number of ads, plot grouped bars and save to file."""
        # Convert to a pandas DataFrame
        df = pd.DataFrame(ads_by_browser_id)

        # Change browser_id to integers from 1 to N conserving the order
        df["browser_id"] = range(1, len(df) + 1)
        df.rename(columns={"browser_id": "Session"})

        print(df)
        # Customizing the plot
        cookie_banner_option_string = ["Ignore", "Accept", "Reject"][
            cookie_banner_option
        ]

        if self.control_runs:
            file_name_suffix = "CONTROL_" + file_name_suffix

        # Write the data to a Markdown table file
        markdown_filename = f"{self.results_dir}/ads_analysis_{cookie_banner_option_string}_{file_name_suffix}.md"
        with open(markdown_filename, "w") as file:
            file.write(df.to_markdown(index=False))

        print(f"Data saved to {markdown_filename}")
        if RESULTS_DIR:
            markdown_filename = f"{RESULTS_DIR}/ads_analysis_{cookie_banner_option_string}_{file_name_suffix}.md"
            with open(markdown_filename, "w") as file:
                file.write(df.to_markdown(index=False))
            print(f"Data saved to {markdown_filename}")
