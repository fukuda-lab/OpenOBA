import sqlite3

import os
import sqlite3
import pandas as pd


import sqlite3
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt

DATA_FROM_VOLUME = False


class OBAQuantifier:
    def __init__(self, experiment_name: str, category_to_analyze: str):
        """Initialize the analyzer with the path to the SQLite database."""
        self.experiment_name = experiment_name
        # self.data_path = f"../datadir/{experiment_name}/crawl-data.sqlite"
        if DATA_FROM_VOLUME:
            self.experiment_data_dir = f"/Volumes/FOBAM_data/28_02_style_and_fashion/datadir/{self.experiment_name}/"
            self.db_path = f"{self.experiment_data_dir}/crawl-data-copy.sqlite"
        else:
            self.experiment_data_dir = f"datadir/{self.experiment_name}/"
            self.db_path = f"{self.experiment_data_dir}/crawl-data.sqlite"

        os.makedirs(f"{self.experiment_data_dir}/results", exist_ok=True)

        self.output_path = (
            f"datadir/{self.experiment_name}/results/chronological_progress.json"
        )
        self.category_to_analyze = category_to_analyze
        self.conn = None

    def connect(self):
        """Establish a connection to the SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Access columns by name

    def disconnect(self):
        """Close the connection to the SQLite database."""
        if self.conn:
            self.conn.close()

    def fetch_visits_ob_ads(self) -> List[Tuple[int, int, int]]:
        """Fetch the count of potentially OBA ads per control site visit, ordered by site rank."""
        query = """
        SELECT 
            sv.visit_id, sv.site_rank, COUNT(DISTINCT va.landing_page_id) as oba_ad_count
        FROM site_visits sv
        JOIN visit_advertisements va ON va.visit_id = sv.visit_id AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
        JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
        JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
        WHERE sv.site_rank > 0 AND lpc.category_name = ? AND va.categorized = TRUE
        GROUP BY sv.visit_id, sv.site_rank
        ORDER BY sv.site_rank
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (self.category_to_analyze,))
        return cursor.fetchall()

    def visualize_ob_advertising_evolution_by_visit(self):
        """Visualize the evolution of potentially OBA exposure per control visit and cumulatively."""
        self.connect()
        try:
            visits_ob_ads = self.fetch_visits_ob_ads()
            site_ranks = [row[1] for row in visits_ob_ads]
            oba_ad_counts = [row[2] for row in visits_ob_ads]
            cumulative_oba_ads = [
                sum(oba_ad_counts[: i + 1]) for i in range(len(oba_ad_counts))
            ]

            plt.figure(figsize=(12, 6))

            # Plot individual visit evolution
            plt.subplot(1, 2, 1)
            plt.plot(site_ranks, oba_ad_counts, marker="o", linestyle="-", color="blue")
            plt.title("OBA Evolution by Control Visit")
            plt.xlabel("Control Visit Site Rank")
            plt.ylabel("Potentially OBA Ads Count")

            # Plot cumulative evolution
            plt.subplot(1, 2, 2)
            plt.plot(
                site_ranks, cumulative_oba_ads, marker="o", linestyle="-", color="green"
            )
            plt.title("Cumulative OBA Evolution")
            plt.xlabel("Control Visit Site Rank")
            plt.ylabel("Cumulative Potentially OBA Ads Count")

            plt.tight_layout()
            plt.show()
        finally:
            self.disconnect()

    def fetch_control_site_ads_breakdown(self) -> Dict[str, List[Dict]]:
        """Fetch ads breakdown for each control site by site_rank."""
        query = """
        SELECT 
            sv.site_url, sv.site_rank,
            COUNT(DISTINCT va.landing_page_id) as categorized_ads_count,
            COUNT(DISTINCT CASE WHEN lpc.category_name = ? THEN va.landing_page_id END) as interest_ads_count
        FROM site_visits sv
        LEFT JOIN visit_advertisements va ON va.visit_id = sv.visit_id AND va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
        LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
        LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
        WHERE sv.site_rank > 0
        GROUP BY sv.site_url, sv.site_rank
        ORDER BY sv.site_url, sv.site_rank
        """
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query, (self.category_to_analyze,))
        data = cursor.fetchall()
        self.disconnect()

        # Organize data by site_url
        organized_data = {}
        for row in data:
            site_data = organized_data.setdefault(row[0], [])
            site_data.append(
                {
                    "site_rank": row[1],
                    "categorized_ads_count": row[2],
                    "interest_ads_count": row[3],
                }
            )

        return organized_data

    def generate_markdown_tables(self, organized_data: Dict[str, List[Dict]]):
        """Generate Markdown tables for each control site's ad breakdown."""
        for site_url, data in organized_data.items():
            cumulative_interest = cumulative_categorized = 0
            print(f"### {site_url}\n")
            print(
                "| Site Rank | Interest Ads | Cumulative Interest Ads | Categorized Ads | Cumulative Categorized Ads |"
            )
            print(
                "|-----------|--------------|-------------------------|-----------------|----------------------------|"
            )
            for row in data:
                cumulative_interest += row["interest_ads_count"]
                cumulative_categorized += row["categorized_ads_count"]
                print(
                    f"| {row['site_rank']} | {row['interest_ads_count']} | {cumulative_interest} | {row['categorized_ads_count']} | {cumulative_categorized} |"
                )
            print("\n")

    def fetch_aggregated_ads_breakdown(self) -> List[Dict]:
        """Fetch aggregated ads breakdown for all control sites."""
        query = """
        SELECT 
            sv.site_rank,
            COUNT(DISTINCT va.landing_page_id) as categorized_ads_count,
            COUNT(DISTINCT CASE WHEN lpc.category_name = ? THEN va.landing_page_id END) as interest_ads_count
        FROM site_visits sv
        LEFT JOIN visit_advertisements va ON va.visit_id = sv.visit_id AND va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
        LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
        LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
        WHERE sv.site_rank > 0
        GROUP BY sv.site_rank
        ORDER BY sv.site_rank
        """
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query, (self.category_to_analyze,))
        data = cursor.fetchall()
        self.disconnect()

        return [
            {
                "site_rank": row[0],
                "categorized_ads_count": row[1],
                "interest_ads_count": row[2],
            }
            for row in data
        ]

    def generate_aggregated_markdown_table(self, aggregated_data: List[Dict]):
        """Generate a Markdown table for the aggregated ad breakdown across all control sites."""
        cumulative_interest = cumulative_categorized = 0
        print("### Aggregated Control Site Data\n")
        print(
            "| Site Rank | Interest Ads | Cumulative Interest Ads | Categorized Ads | Cumulative Categorized Ads |"
        )
        print(
            "|-----------|--------------|-------------------------|-----------------|----------------------------|"
        )
        for row in aggregated_data:
            cumulative_interest += row["interest_ads_count"]
            cumulative_categorized += row["categorized_ads_count"]
            print(
                f"| {row['site_rank']} | {row['interest_ads_count']} | {cumulative_interest} | {row['categorized_ads_count']} | {cumulative_categorized} |"
            )
        print("\n")

    # TODO: FIX IF WANTED TO BE USED
    # def fetch_sessions_ob_ads(self) -> List[Tuple[int, int]]:
    #     """Fetch the count of categorized ads per session (browser_id) from `site_visits`."""
    #     query = """
    #     SELECT
    #         sv.browser_id, COUNT(DISTINCT va.landing_page_id) as ad_count
    #     FROM site_visits sv
    #     JOIN visit_advertisements va ON va.visit_id = sv.visit_id AND va.categorized = TRUE
    #     GROUP BY sv.browser_id
    #     ORDER BY sv.browser_id
    #     """
    #     cursor = self.conn.cursor()
    #     cursor.execute(query)
    #     return cursor.fetchall()

    # def visualize_ob_advertising_evolution_by_session(
    #     self,
    # ):  # TODO: fix the retrieval to include the category of interest
    #     """Visualize the evolution of OBA per session and cumulatively."""
    #     self.connect()
    #     try:
    #         sessions_ob_ads = self.fetch_sessions_ob_ads()
    #         sessions = [row[0] for row in sessions_ob_ads]
    #         ad_counts = [row[1] for row in sessions_ob_ads]
    #         cumulative_ads = [sum(ad_counts[: i + 1]) for i in range(len(ad_counts))]

    #         plt.figure(figsize=(10, 5))

    #         # Plot individual session evolution
    #         plt.subplot(1, 2, 1)
    #         plt.plot(sessions, ad_counts, marker="o", linestyle="-", color="blue")
    #         plt.title("OBA Evolution by Session")
    #         plt.xlabel("Session (Browser ID)")
    #         plt.ylabel("Categorized Ads Count")

    #         # Plot cumulative evolution
    #         plt.subplot(1, 2, 2)
    #         plt.plot(sessions, cumulative_ads, marker="o", linestyle="-", color="green")
    #         plt.title("Cumulative OBA Evolution")
    #         plt.xlabel("Session (Browser ID)")
    #         plt.ylabel("Cumulative Categorized Ads Count")

    #         plt.tight_layout()
    #         plt.show()
    #     finally:
    #         self.disconnect()


# Usage
oba_quantifier = OBAQuantifier(
    experiment_name="test_nohup_style_and_fashion_experiment_accept",
    category_to_analyze="Style & Fashion",
)
# oba_quantifier = OBAQuantifier(
#     experiment_name="style_and_fashion_experiment_accept",
#     category_to_analyze="Style & Fashion",
# )
organized_data = oba_quantifier.fetch_control_site_ads_breakdown()
oba_quantifier.generate_markdown_tables(organized_data)
aggregated_data = oba_quantifier.fetch_aggregated_ads_breakdown()
oba_quantifier.generate_aggregated_markdown_table(aggregated_data)
oba_quantifier.visualize_ob_advertising_evolution_by_visit()


class OBAAnalyzer:
    def __init__(self, experiment_name: str):
        """Initialize the analyzer with the path to the SQLite database."""
        self.experiment_name = experiment_name
        # self.data_path = f"../datadir/{experiment_name}/crawl-data.sqlite"
        self.data_path = f"/Volumes/FOBAM_data/28_02_style_and_fashion/datadir/{self.experiment_name}/crawl-data-copy.sqlite"
        self.conn = sqlite3.connect(self.data_path)

    def _execute_query(self, query):
        """Execute a given SQL query and return the results."""
        cur = self.conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        return results

    def get_visits_summary(self):
        """Retrieve a summary of visits."""
        query = """
        SELECT type, COUNT(*) as num_visits
        FROM visits
        GROUP BY type
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["Type", "NumVisits"])

    def get_ads_summary(self):
        """Retrieve a summary of advertisements."""
        query = """
        SELECT categorized, COUNT(*) as num_ads
        FROM visit_advertisements
        GROUP BY categorized
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["Categorized", "NumAds"])

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
# analyzer = OBAAnalyzer("style_and_fashion_experiment_accept")
# print(analyzer.get_visits_summary())
# print(analyzer.get_ads_summary())
# print(analyzer.get_landing_pages_summary())
# print(analyzer.get_category_distribution())
# analyzer.close()


# Usage example
# analyzer = OBAAnalyzer("style_and_fashion_experiment_accept")
# analyzer.analyze_data()
