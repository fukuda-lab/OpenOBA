import sqlite3

import os
import sqlite3
import pandas as pd


import sqlite3
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt

DATA_FROM_VOLUME = True


class ExperimentMetrics:
    def __init__(self, experiment_name: str):
        """Initialize the analyzer with the path to the SQLite database."""
        self.experiment_name = experiment_name
        # self.data_path = f"../datadir/{experiment_name}/crawl-data.sqlite"
        # self.data_path = f"/Volumes/FOBAM_data/8_days/datadir/{self.experiment_name}/crawl-data-copy.sqlite"
        # self.conn = sqlite3.connect(self.data_path)
        self.data_path = (
            f"/Volumes/FOBAM_data/control_runs/{self.experiment_name}/crawl-data.sqlite"
        )
        self.conn = sqlite3.connect(self.data_path)

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

    def get_control_visits_by_url_and_browser(self):
        """Retrieve a summary of visits."""
        query = """
            SELECT site_url, COUNT(*) as num_visits
            FROM site_visits
            WHERE site_url IN (
                'http://myforecast.com/',
                'http://weatherbase.com/',
                'http://theweathernetwork.com/',
                'http://weather.com/',
                'http://weather2umbrella.com/'
            )
            GROUP BY site_url
            ORDER BY site_url;
        """
        results = self._execute_query(query)
        return pd.DataFrame(results, columns=["URL", "NumVisits"])

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
