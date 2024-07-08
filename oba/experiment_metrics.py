import json
import sqlite3

import os
import sqlite3
import pandas as pd
import tldextract


import sqlite3
from typing import List, Dict, Tuple
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from oba.enums import (
    IAB_CATEGORIES,
    NOTHING_GROUP,
    A_GROUP,
    M_GROUP,
    M_MINUS_GROUP,
    U_GROUP,
)

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
            self.experiment_dir = f"./datadir/{experiment_name}"
            self.data_path = f"{self.experiment_dir}/crawl-data.sqlite"
            print(self.data_path)

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
        # SQL query to join the tables and get the required fields
        query = """
        SELECT
            va.ad_id,
            va.ad_url,
            va.landing_page_url,
            lp.category_id,
            lp.category_name,
            sv.site_url,
            va.visit_id,
            va.browser_id
        FROM
            visit_advertisements va
        LEFT JOIN
            landing_page_categories lp ON va.landing_page_id = lp.landing_page_id
        LEFT JOIN
            site_visits sv ON va.visit_id = sv.visit_id
        WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND va.clean_run = FALSE
        """

        # Execute the query and load the data into a DataFrame
        df = pd.read_sql_query(query, self.conn)

        # Group by ad_id and aggregate the categories
        df_grouped = (
            df.groupby("ad_id")
            .agg(
                {
                    "ad_url": "first",
                    "landing_page_url": "first",
                    "category_name": lambda x: list(x.dropna().unique()),
                    "site_url": "first",
                    "visit_id": "first",
                    "browser_id": "first",
                }
            )
            .reset_index()
        )

        # Rename the category_name column to categories
        df_grouped.rename(columns={"category_name": "categories"}, inplace=True)

        # Last two browser_ids
        first_6_browser_ids = self.oba_browsers[:6]

        # Filter out the last two browser_ids
        df_grouped = df_grouped[df_grouped["browser_id"].isin(first_6_browser_ids)]

        # print(df_grouped)
        # Extract the domain from the ad_url domain and landing_page_url domain

        df_grouped["adurl_domain"] = df_grouped["ad_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # Extract the domain from the landing_page_url domain
        df_grouped["landingpage_domain"] = df_grouped["landing_page_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )
        # return df_grouped

        df_grouped["group"] = np.where(
            df_grouped["adurl_domain"] != df_grouped["landingpage_domain"], "A", "Other"
        )

        # Create new DataFrame of ads, keeping only the ads whose adurl_domain have at least 1 ad in the A group
        df_grouped_filtered = df_grouped[
            df_grouped["adurl_domain"].isin(
                df_grouped[df_grouped["group"] == "A"]["adurl_domain"].unique()
            )
        ]

        return df_grouped_filtered

    def get_ads_by_category_table_all_browsers_old(self) -> List[Dict]:
        """Old way of counting ads by category"""

        # SQL query to join the tables and get the required fields
        query = """
        SELECT
            va.ad_id,
            va.ad_url,
            va.landing_page_url,
            lp.category_id,
            lp.category_name,
            sv.site_url,
            va.visit_id,
            va.browser_id
        FROM
            visit_advertisements va
        LEFT JOIN
            landing_page_categories lp ON va.landing_page_id = lp.landing_page_id
        LEFT JOIN
            site_visits sv ON va.visit_id = sv.visit_id
        WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND va.clean_run = FALSE
        """

        # Execute the query and load the data into a DataFrame
        df = pd.read_sql_query(query, self.conn)

        # Group by ad_id and aggregate the categories
        df_grouped = (
            df.groupby("ad_id")
            .agg(
                {
                    "ad_url": "first",
                    "landing_page_url": "first",
                    "category_name": lambda x: list(x.dropna().unique()),
                    "site_url": "first",
                    "visit_id": "first",
                    "browser_id": "first",
                }
            )
            .reset_index()
        )

        # Rename the category_name column to categories
        df_grouped.rename(columns={"category_name": "categories"}, inplace=True)

        # Last two browser_ids
        first_6_browser_ids = self.oba_browsers[:6]

        # Filter out the last two browser_ids
        df_grouped = df_grouped[df_grouped["browser_id"].isin(first_6_browser_ids)]

        # print(df_grouped)
        return df_grouped

    def get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
        self,
        numvisits_by_browser_id_and_url: List[Tuple[int, str, int]],
    ):
        if not self.conn:
            self.connect()

        # Step 1: Extract the visit data
        site_visits_query = """
        SELECT visit_id, browser_id, site_url, site_rank
        FROM site_visits
        ORDER BY site_rank;
        """
        site_visits_df = pd.read_sql(site_visits_query, self.conn)

        # Step 2: Map the browser_id based on the visit counts
        # Create a dictionary to store the count of visits
        browser_visit_counts = {}
        for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
            if browser_id not in browser_visit_counts:
                browser_visit_counts[browser_id] = []
            browser_visit_counts[browser_id] += [site_url] * num_visits

        # Flatten the dictionary to a list of tuples (browser_id, site_url)
        flattened_visits = [
            (browser_id, site_url)
            for browser_id, visits in browser_visit_counts.items()
            for site_url in visits
        ]

        # Add the browser_id to the site_visits_df based on the site_url and site_rank
        site_visits_df["browser_id"] = None

        for (browser_id, site_url), site_rank in zip(
            flattened_visits, site_visits_df["site_rank"]
        ):
            site_visits_df.loc[
                (site_visits_df["site_url"] == site_url)
                & (site_visits_df["site_rank"] == site_rank),
                "browser_id",
            ] = browser_id

        # Step 3: Join the relevant tables to get the final DataFrame

        ads_query = """
        SELECT 
            va.ad_id, 
            va.ad_url, 
            va.landing_page_url, 
            va.visit_url, 
            va.visit_id, 
            sv.site_rank, 
            sv.browser_id,
            lpc.category_name
        FROM visit_advertisements va
        JOIN site_visits sv ON va.visit_id = sv.visit_id
        LEFT JOIN landing_page_categories lpc ON va.landing_page_id = lpc.landing_page_id
        WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
        """
        ads_df = pd.read_sql(ads_query, self.conn)

        # Group the categories by ad_id
        categories_df = (
            ads_df.groupby("ad_id")["category_name"].apply(list).reset_index()
        )

        # Change the browser_id column from the database to the browser_id from the visit data
        ads_df["browser_id"] = ads_df["site_rank"].apply(
            lambda x: site_visits_df.loc[x - 1, "browser_id"]
        )

        # Merge the categories back into the ads_df
        final_df = pd.merge(
            ads_df.drop(columns=["category_name"]),
            categories_df,
            on="ad_id",
            how="left",
        )

        # Closing the connection
        self.conn.close()

        # Rename the category_name column to categories
        final_df.rename(columns={"category_name": "categories"}, inplace=True)

        # Get all the browser_ids from numvisits_by_browser_id_and_url
        browser_ids = []
        for browser_id, _, _ in numvisits_by_browser_id_and_url:
            if browser_id not in browser_ids:
                browser_ids.append(browser_id)
        first_6_browser_ids = browser_ids[:6]

        # Filter out the last two browser_ids
        final_df = final_df[final_df["browser_id"].isin(first_6_browser_ids)]

        # print(final_df)

        return final_df

    def get_categories_total_and_unique_ads(self, ads_df):
        # Ensure categories column contains lists of categories
        ads_df["categories"] = ads_df["categories"].apply(
            lambda x: x if isinstance(x, list) else []
        )

        # Flatten the dataframe to have one row per category per ad
        flattened_ads = ads_df.explode("categories")

        # Calculate NumTotalAds: unique ad_urls per each combination of visit_id and browser_id
        total_ads = (
            flattened_ads.groupby(["visit_id", "browser_id", "categories"])["ad_url"]
            .nunique()
            .reset_index()
        )
        total_ads_summary = (
            total_ads.groupby("categories")["ad_url"].sum().reset_index()
        )
        total_ads_summary.columns = ["Category", "NumTotalAds"]

        # Calculate NumUniqueAds: unique ad_urls among all the ads
        unique_ads_summary = (
            flattened_ads.groupby("categories")["ad_url"].nunique().reset_index()
        )
        unique_ads_summary.columns = ["Category", "NumUniqueAds"]

        # Merge the summaries
        summary_df = pd.merge(total_ads_summary, unique_ads_summary, on="Category")

        # Filter out Tier 2 categories:
        # Add a new column with a boolean indicating if the category is tier 1 or not
        tier1_categories = []
        for key, value in IAB_CATEGORIES.items():
            tier1_categories.append(value[key])

        summary_df.insert(1, "IsTier1", summary_df["Category"].isin(tier1_categories))

        # Update summary_df
        summary_df = summary_df[summary_df["IsTier1"]]

        # Calculate total number of ads for percentage calculation from all ads before grouping by category
        # For total all ads, we need to count the sum of unique ad_urls per each combination of visit_id and browser_id
        total_ads_all = (
            ads_df.groupby(["visit_id", "browser_id"])["ad_url"].nunique().sum()
        )
        unique_ads_all = ads_df["ad_url"].nunique()

        # Calculate percentages
        summary_df["PercentageFromAllTotalAds"] = (
            summary_df["NumTotalAds"] / total_ads_all
        ) * 100
        summary_df["PercentageFromAllUniqueAds"] = (
            summary_df["NumUniqueAds"] / unique_ads_all
        ) * 100

        # Order by total ads
        summary_df = summary_df.sort_values(by="NumTotalAds", ascending=False)

        # Reset the index
        summary_df.reset_index(drop=True, inplace=True)

        # Add a last row with the total number of ads
        total_row = pd.DataFrame(
            {
                "Category": "Any",
                "NumTotalAds": total_ads_all,
                "NumUniqueAds": unique_ads_all,
                "IsTier1": False,
                "PercentageFromAllTotalAds": 100,
                "PercentageFromAllUniqueAds": 100,
            },
            index=[len(summary_df) + 1],
        )

        # Calculate the total and unique ads that are not "Style & Fashion" nor "Shopping" at the same time
        total_ads_not_style_nor_shopping = (
            ads_df[
                ads_df["categories"].apply(
                    lambda x: "Style & Fashion" not in x and "Shopping" not in x
                )
            ]
            .groupby(["visit_id", "browser_id"])["ad_url"]
            .nunique()
            .sum()
        )
        unique_ads_not_style_nor_shopping = ads_df[
            ads_df["categories"].apply(
                lambda x: "Style & Fashion" not in x and "Shopping" not in x
            )
        ]["ad_url"].nunique()

        # Add a last row with the total number of ads that are not "Style & Fashion" nor "Shopping" at the same time
        total_row_not_style_nor_shopping = pd.DataFrame(
            {
                "Category": "Not Style & Fashion / Shopping",
                "NumTotalAds": total_ads_not_style_nor_shopping,
                "NumUniqueAds": unique_ads_not_style_nor_shopping,
                "IsTier1": False,
                "PercentageFromAllTotalAds": (
                    total_ads_not_style_nor_shopping / total_ads_all
                )
                * 100,
                "PercentageFromAllUniqueAds": (
                    unique_ads_not_style_nor_shopping / unique_ads_all
                )
                * 100,
            },
            index=[len(summary_df)],
        )

        full_summary_df = pd.concat(
            [summary_df, total_row_not_style_nor_shopping, total_row]
        )

        # Save the results to a markdown file
        full_summary_df.to_markdown(
            f"{self.experiment_dir}/results/ads_by_category.md", index=False
        )

        return summary_df

    def get_ads_evolution_by_session(
        self, df, oba_browsers, category_filter="Style & Fashion"
    ):
        if len(oba_browsers) > 6:
            oba_browsers = oba_browsers[:6]

        def calculate_session_metrics(df, session_id):
            session_data = df[df["browser_id"] == session_id]

            # Calculate metrics by visit
            visits = session_data["visit_id"].unique()
            total_unique_ads_by_visit = 0
            style_unique_ads_by_visit = 0

            for visit in visits:
                visit_data = session_data[session_data["visit_id"] == visit]
                total_unique_ads = visit_data["ad_url"].nunique()
                style_unique_ads = visit_data[
                    visit_data["categories"].apply(lambda x: category_filter in x)
                ]["ad_url"].nunique()

                total_unique_ads_by_visit += total_unique_ads
                style_unique_ads_by_visit += style_unique_ads

            percentage_by_visit = (
                (style_unique_ads_by_visit / total_unique_ads_by_visit) * 100
                if total_unique_ads_by_visit > 0
                else 0
            )

            # Calculate metrics for the entire session
            total_unique_ads_session = session_data["ad_url"].nunique()
            style_unique_ads_session = session_data[
                session_data["categories"].apply(lambda x: category_filter in x)
            ]["ad_url"].nunique()

            percentage_session = (
                (style_unique_ads_session / total_unique_ads_session) * 100
                if total_unique_ads_session > 0
                else 0
            )

            return {
                "style_unique_ads_by_visit": style_unique_ads_by_visit,
                "total_unique_ads_by_visit": total_unique_ads_by_visit,
                "percentage_by_visit": percentage_by_visit,
                "style_unique_ads_by_session": style_unique_ads_session,
                "total_unique_ads_by_session": total_unique_ads_session,
                "percentage_session": percentage_session,
            }

        # Initialize results list
        results = []

        # Process each session based on the browser_ids list
        for session_number, browser_id in enumerate(oba_browsers, 1):
            metrics = calculate_session_metrics(df, browser_id)
            metrics["session_number"] = session_number
            results.append(metrics)

        # Create the results DataFrame
        results_df = pd.DataFrame(results)

        return results_df

    def count_ads_by_session_ad_provider_vs_other(self, ads_df, category_filter=None):
        """Then we add a new column with the domain of the ad_url, and with that column we create a new one with the group the domain belongs to. Finally we group by the session and count the number of ads per group."""
        if category_filter:
            ads_df = ads_df[ads_df["categories"].apply(lambda x: category_filter in x)]

        # Extract the domain from the ad_url domain and landing_page_url domain
        ads_df["adurl_domain"] = ads_df["ad_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )
        ads_df["landingpage_domain"] = ads_df["landing_page_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # ---OLD APPROACH---
        # Create a new column with the group the domain belongs to
        # domain_groups = {
        #     "Nothing": NOTHING_GROUP,
        #     "A": A_GROUP,
        #     "M": M_GROUP,
        #     "M-": M_MINUS_GROUP,
        #     "U": U_GROUP,
        # }

        # def get_group(domain):
        #     for group, domains in domain_groups.items():
        #         if domain in domains:
        #             return group
        #     return None

        # ads_df["group"] = ads_df["adurl_domain"].apply(lambda x: get_group(x))

        # ---NEW APPROACH---
        # Create a new column with the group the domain belongs to
        # If adurl_domain is different from landingpage_domain, then it is A group,
        # If adurl_domain is equal to landingpage_domain, then it is Other group
        ads_df["group"] = np.where(
            ads_df["adurl_domain"] != ads_df["landingpage_domain"], "A", "Other"
        )

        # if not category_filter:
        # Write down a csv file with all the unique adurl_domain and a column with the group, ordered by group
        # ads_df[["adurl_domain", "group"]].drop_duplicates().to_csv(
        # f"{self.experiment_dir}/results/ad_domains_all.csv", index=False
        # )

        # For all adurl domains that have an ad in the A group, change the group of every ad with that domain to A
        a_domains = ads_df[ads_df["group"] == "A"]["adurl_domain"].unique()
        ads_df.loc[ads_df["adurl_domain"].isin(a_domains), "group"] = "A"

        # group by session, visit, and group, and count the number of unique ads
        grouped_ads = (
            ads_df.groupby(["browser_id", "visit_id", "group"])["ad_url"]
            .nunique()
            .reset_index()
            .rename(columns={"ad_url": "NumAds"})
        )

        # Now group by session and group and count the number of ads
        grouped_ads = (
            grouped_ads.groupby(["browser_id", "group"])["NumAds"].sum().reset_index()
        )

        # Order the groups by the self.browser_ids order
        first_6_oba_browsers = self.oba_browsers[:6]

        grouped_ads["browser_id"] = pd.Categorical(
            grouped_ads["browser_id"], categories=first_6_oba_browsers, ordered=True
        )
        grouped_ads = grouped_ads.sort_values(by=["browser_id", "group"])

        # Now generate two 1x6 DataFrames with the columns being each session:
        # One with the total number of ads in group A
        # And the other with the total number of ads in every other group
        total_ads_group_a = (
            grouped_ads[grouped_ads["group"] == "A"]
            .groupby("browser_id")["NumAds"]
            .sum()
            .values
        )
        total_ads_other_groups = (
            grouped_ads[grouped_ads["group"] != "A"]
            .groupby("browser_id")["NumAds"]
            .sum()
            .values
        )

        result = (list(total_ads_group_a), list(total_ads_other_groups))

        return result

    def count_ads_by_session_any_provider(self, ads_df, category_filter=None):
        """Then we add a new column with the domain of the ad_url, and with that column we create a new one with the group the domain belongs to. Finally we group by the session and count the number of ads per group."""
        if category_filter:
            ads_df = ads_df[ads_df["categories"].apply(lambda x: category_filter in x)]

        # Extract the domain from the ad_url domain and landing_page_url domain
        ads_df["adurl_domain"] = ads_df["ad_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # Extract the domain from the landing_page_url domain
        ads_df["landingpage_domain"] = ads_df["landing_page_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # group by session and visit and count the number of unique ads
        grouped_ads = (
            ads_df.groupby(["browser_id", "visit_id"])["ad_url"]
            .nunique()
            .reset_index()
            .rename(columns={"ad_url": "NumAds"})
        )

        # Now group by session and count the number of ads
        grouped_ads = grouped_ads.groupby(["browser_id"])["NumAds"].sum().reset_index()

        # Order the groups by the self.browser_ids order
        first_6_oba_browsers = self.oba_browsers[:6]

        grouped_ads["browser_id"] = pd.Categorical(
            grouped_ads["browser_id"], categories=first_6_oba_browsers, ordered=True
        )
        grouped_ads = grouped_ads.sort_values(by=["browser_id"])

        # Now transform the DataFrame to a list of the total number of ads per session, and fill with 0 if the session has no ads
        total_ads = []
        for browser_id in first_6_oba_browsers:
            try:
                total_ads.append(
                    grouped_ads[grouped_ads["browser_id"] == browser_id][
                        "NumAds"
                    ].values[0]
                )
            except IndexError:
                total_ads.append(0)

        return total_ads

    def count_ads_by_session(self, ads_df, category_filter=None):
        """Then we add a new column with the domain of the ad_url, and with that column we create a new one with the group the domain belongs to. Finally we group by the session and count the number of ads per group."""
        if category_filter:
            ads_df = ads_df[ads_df["categories"].apply(lambda x: category_filter in x)]

        # Extract the domain from the ad_url domain and landing_page_url domain
        ads_df["adurl_domain"] = ads_df["ad_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # Extract the domain from the landing_page_url domain
        ads_df["landingpage_domain"] = ads_df["landing_page_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # group by session, visit and domain and count the number of unique ads
        grouped_ads = (
            ads_df.groupby(["browser_id", "visit_id", "adurl_domain"])["ad_url"]
            .nunique()
            .reset_index()
            .rename(columns={"ad_url": "NumAds"})
        )

        # Now group by session and domain and count the number of ads
        grouped_ads = (
            grouped_ads.groupby(["browser_id", "adurl_domain"])["NumAds"]
            .sum()
            .reset_index()
        )

        # Order the groups by the self.browser_ids order
        first_6_oba_browsers = self.oba_browsers[:6]

        grouped_ads["browser_id"] = pd.Categorical(
            grouped_ads["browser_id"], categories=first_6_oba_browsers, ordered=True
        )
        grouped_ads = grouped_ads.sort_values(by=["browser_id"])

        # We have two domains that we care about: doubleclick.net and googleadservices.com
        # We want to return a list with the total number of ads per session for each domain
        grouped_ads = (
            grouped_ads[
                grouped_ads["adurl_domain"].isin(
                    ["doubleclick.net", "googleadservices.com"]
                )
            ]
            .groupby(["browser_id", "adurl_domain"])["NumAds"]
            .sum()
        )

        list_googleadservices = []
        list_doubleclick = []

        for browser_id in first_6_oba_browsers:
            try:
                list_googleadservices.append(
                    grouped_ads[browser_id, "googleadservices.com"]
                )
            except KeyError:
                list_googleadservices.append(0)
            try:
                list_doubleclick.append(grouped_ads[browser_id, "doubleclick.net"])
            except KeyError:
                list_doubleclick.append(0)

        print(list_googleadservices)
        print(list_doubleclick)

        return (list_googleadservices, list_doubleclick)

    def count_ads_by_domain(self, ads_df, category_filter=None):
        if category_filter:
            ads_df = ads_df[ads_df["categories"].apply(lambda x: category_filter in x)]

        # Extract the domain from the ad_url domain and landing_page_url domain
        ads_df["adurl_domain"] = ads_df["ad_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # Extract the domain from the landing_page_url domain
        ads_df["landingpage_domain"] = ads_df["landing_page_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # group by session and visit and count the number of unique ads
        grouped_ads = (
            ads_df.groupby(["browser_id", "visit_id", "adurl_domain"])["ad_url"]
            .nunique()
            .reset_index()
            .rename(columns={"ad_url": "NumAds"})
        )

        # Now group by domain and count the number of ads, ordered by the number of ads
        grouped_ads = grouped_ads.groupby("adurl_domain")["NumAds"].sum().reset_index()
        grouped_ads = grouped_ads.sort_values(by="NumAds", ascending=False)

        # Write down csv file with the domains and the number of ads
        grouped_ads.to_csv(
            f"{self.experiment_dir}/results/ad_domains_{category_filter if category_filter else 'all_categories'}.csv",
            index=False,
        )

        # Now do the same thing but filtering by: If any domain has at least 1 ad whose ad_url_domain != landing_page_domain, keep it, otherwise, removed it
        # Create a new column with the group the domain belongs to
        # If adurl_domain is different from landingpage_domain, then it is A group,
        # If adurl_domain is equal to landingpage_domain, then it is Other group
        ads_df["group"] = np.where(
            ads_df["adurl_domain"] != ads_df["landingpage_domain"], "A", "Other"
        )

        # Create new DataFrame of ads, keeping only the ads whose adurl_domain have at least 1 ad in the A group
        ads_df_filtered = ads_df[
            ads_df["adurl_domain"].isin(
                ads_df[ads_df["group"] == "A"]["adurl_domain"].unique()
            )
        ]

        # group by session and visit and count the number of unique ads
        grouped_ads_filtered = (
            ads_df_filtered.groupby(["browser_id", "visit_id", "adurl_domain"])[
                "ad_url"
            ]
            .nunique()
            .reset_index()
            .rename(columns={"ad_url": "NumAds"})
        )

        # Now group by domain and count the number of ads, ordered by the number of ads
        # grouped_ads_filtered = (
        #     grouped_ads_filtered.groupby("adurl_domain")["NumAds"].sum().reset_index()
        # )
        # grouped_ads_filtered = grouped_ads_filtered.sort_values(
        #     by="NumAds", ascending=False
        # )

        # Write down csv file with the domains and the number of ads
        # grouped_ads_filtered.to_csv(
        #     f"{self.experiment_dir}/results/ads_filtered_domains_{category_filter if category_filter else 'all_categories'}.csv",
        #     index=False,
        # )

        # Now do the exact same thing but for ad_url instead of adurl_domain.
        # We want to have a csv file with all the unique ad_urls together with the number of unique visits in which they appeared.
        grouped_ads_adurl = (
            ads_df.groupby(["ad_url"])["visit_id"]
            .nunique()
            .reset_index()
            .rename(columns={"visit_id": "NumVisits"})
        )
        grouped_ads_adurl = grouped_ads_adurl.sort_values(
            by="NumVisits", ascending=False
        )

        # Write down csv file with the ad_urls and the number of visits
        grouped_ads_adurl.to_csv(
            f"{self.experiment_dir}/results/ad_urls_{category_filter if category_filter else 'all_categories'}.csv",
            index=False,
        )

        return grouped_ads_filtered

    def count_ads_by_session_by_group(self, ads_df, category_filter=None):
        """Then we add a new column with the domain of the ad_url, and with that column we create a new one with the group the domain belongs to. Finally we group by the session and count the number of ads per group."""
        if category_filter:
            ads_df = ads_df[ads_df["categories"].apply(lambda x: category_filter in x)]

        # Extract the domain from the ad_url domain and landing_page_url domain
        ads_df["adurl_domain"] = ads_df["ad_url"].apply(
            lambda x: tldextract.extract(x).registered_domain
        )

        # Create a new column with the group the domain belongs to
        domain_groups = {
            "Nothing": NOTHING_GROUP,
            "A": A_GROUP,
            "M": M_GROUP,
            "M-": M_MINUS_GROUP,
            "U": U_GROUP,
        }

        def get_group(domain):
            for group, domains in domain_groups.items():
                if domain in domains:
                    return group
            print(domain)
            return "Nothing"

        ads_df["group"] = ads_df["adurl_domain"].apply(lambda x: get_group(x))

        # group by session, visit, and group, and count the number of unique ads
        grouped_ads_total = (
            ads_df.groupby(["browser_id", "visit_id", "group"])["ad_url"]
            .nunique()
            .reset_index()
            .rename(columns={"ad_url": "NumAds"})
        )

        # Now group by group and count the number of ads
        grouped_ads_total = (
            grouped_ads_total.groupby("group")["NumAds"].sum().reset_index()
        )

        # Now generate an 1x6 DataFrames with the columns being each group
        # And the values being the total number of ads in that group
        result_df_total = grouped_ads_total.set_index("group").T

        # print(result_df_total)

        # Now do the same but for unique ads among all sessions and visits
        grouped_ads_unique = ads_df.groupby(["group"])["ad_url"].nunique().reset_index()

        # Now generate an 1x6 DataFrames with the columns being each group
        # And the values being the total number of unique ads in that group
        result_df_unique = grouped_ads_unique.set_index("group").T

        print(self.experiment_name)
        # Print result_df_total in the following format: & {A} & {M} & {M-} & {U} & {Nothing} \\ \hline
        print(
            f"& {result_df_total['A'].values[0]} & {result_df_total['M'].values[0]} & {result_df_total['M-'].values[0]} & {result_df_total['U'].values[0]} & {result_df_total['Nothing'].values[0]} \\\\ \\hline"
        )

        # Print result_df_unique in the following format: & {A} & {M} & {M-} & {U} & {Nothing} \\ \hline
        print(
            f"& {result_df_unique['A'].values[0]} & {result_df_unique['M'].values[0]} & {result_df_unique['M-'].values[0]} & {result_df_unique['U'].values[0]} & {result_df_unique['Nothing'].values[0]} \\\\ \\hline"
        )

        return (result_df_total, result_df_unique)

    def get_top_10_domains_of_ads_grouped_by_artificial_sessions_and_site_url(
        self,
        numvisits_by_browser_id_and_url: List[Tuple[int, str, int]],
    ) -> List[Dict]:

        def fetch_ads_by_limits_and_site_url(
            cursor,
            site_url: str,
            num_visits: int,
            already_taken: int,
        ):

            query = """
                WITH RankedVisits AS (
                    SELECT
                        sv.visit_id,
                        sv.site_rank,
                        ROW_NUMBER() OVER (PARTITION BY sv.site_url ORDER BY sv.site_rank) AS visit_order
                    FROM site_visits sv
                    WHERE sv.site_url = :site_url
                ),
                SelectedVisits AS (
                    SELECT visit_id
                    FROM RankedVisits
                    WHERE visit_order > :already_taken AND visit_order <= :already_taken + :num_visits
                )
                SELECT va.ad_url
                FROM visit_advertisements va
                JOIN SelectedVisits ON va.visit_id = SelectedVisits.visit_id
                WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
                """
            # WHERE va.non_ad IS NULL AND va.unspecific_ad IS NULL;

            data = {
                "site_url": site_url,
                "num_visits": num_visits,
                "already_taken": already_taken,
            }

            cursor.execute(query, data)

            result = cursor.fetchall()
            return result

        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        # Initialize the sessions list and the set of seen browser_ids
        site_visits_count = {
            site_url: 0 for _, site_url, _ in numvisits_by_browser_id_and_url
        }

        session_number = 0  # Initialize the session number
        seen_browser_ids = set()

        df = pd.DataFrame(columns=["AdURL", "Domain", "Session"])
        for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
            if browser_id not in seen_browser_ids:
                # Move to the next session
                session_number += 1

                # Add the session to seen sessions
                seen_browser_ids.add(browser_id)

            if session_number == 7:
                # We only want to analyze until session 6
                break

                # Fetch the ads from num_visits visits to the current site_url, starting from the first visit after the already taken visit
            query_result = fetch_ads_by_limits_and_site_url(
                cursor,
                site_url,
                num_visits,
                site_visits_count[site_url],
            )

            for ad_url_result in query_result:
                ad_url = ad_url_result[0]
                domain = tldextract.extract(ad_url).registered_domain

                # Append the row to the DataFrame
                row = pd.DataFrame(
                    [{"AdURL": ad_url, "Domain": domain, "Session": session_number}]
                )
                df = pd.concat([df, row])

            # Update the number of visits taken from the site_url
            site_visits_count[site_url] += num_visits

        cursor.close()

        # Group by 'Domain' and aggregate
        domain_summary = (
            df.groupby("Domain")
            .agg(
                Total_AdURLs=("AdURL", "count"),  # Count total AdURLs per domain
                Unique_AdURLs=(
                    "AdURL",
                    pd.Series.nunique,
                ),  # Count unique AdURLs per domain
            )
            .reset_index()
        )  # Reset index to make 'Domain' a column

        # Sort by 'Unique_AdURLs' in descending order
        domain_summary = domain_summary.sort_values(by="Unique_AdURLs", ascending=False)

        # Print all the domains as a list of strings
        # print(domain_summary["Domain"].to_list())

        # Write the results to a markdown table file
        domain_summary.to_markdown(
            f"{self.experiment_dir}/results/top_domains_ads.md", index=False
        )

        # Create a new dataframe whose rows are the different groups of domains and the columns are the number of ads and unique ads for each group
        domain_groups = {
            "Nothing": NOTHING_GROUP,
            "A": A_GROUP,
            "M": M_GROUP,
            "M-": M_MINUS_GROUP,
            "U": U_GROUP,
        }

        # Create a new dataframe with the domain groups
        df_domain_groups = pd.DataFrame(columns=["Group", "TotalAds", "TotalUniqueAds"])

        for group, domains in domain_groups.items():
            df_group = domain_summary[domain_summary["Domain"].isin(domains)]
            total_ads = df_group["Total_AdURLs"].sum()
            total_unique_ads = df_group["Unique_AdURLs"].sum()
            row = pd.DataFrame(
                [
                    {
                        "Group": group,
                        "TotalAds": total_ads,
                        "TotalUniqueAds": total_unique_ads,
                    }
                ]
            )
            df_domain_groups = pd.concat([df_domain_groups, row])

        # Write the results to a markdown table file
        df_domain_groups.to_markdown(
            f"{self.experiment_dir}/results/domain_groups_ads.md", index=False
        )

        return domain_summary

    def get_experiment_summary(
        self, is_control_run, numvisits_by_browser_id_and_url=None
    ):
        """Retrieve a summary of visits."""

        # 1. Get the number of control and training visits
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

        # 2. Now get numbers about the ads

        if not is_control_run:
            ads_query = """
                SELECT 
                    va.visit_id as visit_id,
                    va.ad_id as ad_id,
                    va.ad_url as ad_url,
                    va.browser_id as browser_id,
                    va.landing_page_url as landing_page_url,
                    lpc.category_name as category_name,
                    va.non_ad as non_ad,
                    va.unspecific_ad as unspecific_ad,
                    va.categorized as categorized,
                    lpc.confident as confident
                FROM visit_advertisements va
                JOIN site_visits sv ON va.visit_id = sv.visit_id
                LEFT JOIN landing_page_categories lpc ON va.landing_page_id = lpc.landing_page_id
                WHERE va.clean_run = FALSE
            """
            ads_df = pd.read_sql_query(ads_query, self.conn)

            oba_browsers = self.oba_browsers[:6]

        else:
            # Step 1: Extract the visit data
            site_visits_query = """
            SELECT visit_id, browser_id, site_url, site_rank
            FROM site_visits
            ORDER BY site_rank;
            """
            site_visits_df = pd.read_sql(site_visits_query, self.conn)

            # Step 2: Map the browser_id based on the visit counts
            # Create a dictionary to store the count of visits
            browser_visit_counts = {}
            for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
                if browser_id not in browser_visit_counts:
                    browser_visit_counts[browser_id] = []
                browser_visit_counts[browser_id] += [site_url] * num_visits

            # Flatten the dictionary to a list of tuples (browser_id, site_url)
            flattened_visits = [
                (browser_id, site_url)
                for browser_id, visits in browser_visit_counts.items()
                for site_url in visits
            ]

            # Add the browser_id to the site_visits_df based on the site_url and site_rank
            site_visits_df["browser_id"] = None

            for (browser_id, site_url), site_rank in zip(
                flattened_visits, site_visits_df["site_rank"]
            ):
                site_visits_df.loc[
                    (site_visits_df["site_url"] == site_url)
                    & (site_visits_df["site_rank"] == site_rank),
                    "browser_id",
                ] = browser_id

            # Step 3: Join the relevant tables to get the final DataFrame

            ads_query = """
            SELECT 
                va.visit_id as visit_id, 
                va.ad_id as ad_id, 
                va.ad_url as ad_url, 
                sv.browser_id as browser_id,
                lpc.category_name as category_name,
                va.non_ad as non_ad,
                va.unspecific_ad as unspecific_ad,
                va.categorized as categorized,
                lpc.confident as confident,
                va.visit_url as visit_url, 
                sv.site_rank as site_rank, 
                va.landing_page_url as landing_page_url
            FROM visit_advertisements va
            JOIN site_visits sv ON va.visit_id = sv.visit_id
            LEFT JOIN landing_page_categories lpc ON va.landing_page_id = lpc.landing_page_id
            """
            ads_df = pd.read_sql(ads_query, self.conn)

            # Get the first 6 browser_ids as the OBA browsers from numvisits_by_browser_id_and_url
            oba_browsers = []
            for browser_id, _, _ in numvisits_by_browser_id_and_url:
                if browser_id not in oba_browsers:
                    oba_browsers.append(browser_id)
            oba_browsers = oba_browsers[:6]

            # Change the browser_id column from the database to the browser_id from the visit data
            ads_df["browser_id"] = ads_df["site_rank"].apply(
                lambda x: site_visits_df.loc[x - 1, "browser_id"]
            )

        # Only consider the first 6 browser_ids
        ads_df = ads_df[ads_df["browser_id"].isin(oba_browsers[:6])]

        # First we get the number of ads
        num_ads = ads_df["ad_id"].nunique()

        # Then we group by visit_id and we get the number of unique ads per visit
        num_deduplicated_ads = (
            ads_df.groupby(["browser_id", "visit_id"])["ad_url"].nunique().sum()
        )

        # Now we filter out the non-ads and the unspecific ads from the grouped_ads
        filtered_df = ads_df[
            (ads_df["non_ad"].isnull())
            & (ads_df["unspecific_ad"].isnull())
            & (ads_df["categorized"] == 1)
        ]

        # We group by visit_id and we get the filtered number of unique ads per visit
        num_filtered_ads = filtered_df.groupby("visit_id")["ad_url"].nunique().sum()

        # Now we get the filtered number of unique ads per visit that are categorized
        categorized_filtered_df = filtered_df[
            filtered_df["categorized"]
            == 1 & (filtered_df["category_name"] != "Uncategorized")
        ]
        num_categorized_filtered_ads = (
            categorized_filtered_df.groupby("visit_id")["ad_url"].nunique().sum()
        )

        # Now we get the amount of ads that at least one of the categories is confident
        confident_ads_df = categorized_filtered_df[
            categorized_filtered_df["confident"] == 1
        ]
        num_confident_ads = (
            confident_ads_df.groupby("visit_id")["ad_url"].nunique().sum()
        )

        # Finally we make a markdown table with the results
        summary_df = pd.DataFrame(
            {
                "TotalAds": [num_ads],
                "DeduplicatedAds": [num_deduplicated_ads],
                "FilteredAds": [num_filtered_ads],
                "CategorizedFilteredAds": [num_categorized_filtered_ads],
                "ConfidentAds": [num_confident_ads],
            }
        )

        # Write the results to a markdown file
        summary_df.to_markdown(
            f"{self.experiment_dir}/results/experiment_summary.md", index=False
        )

        return summary_df

    # def get_experiment_summary(self):
    #     """Retrieve a summary of visits."""

    #     # First we get the number of control and training visits
    #     query = """
    #     SELECT
    #         COUNT(CASE WHEN site_rank IS NULL THEN 1 ELSE NULL END) as training_visits,
    #         COUNT(CASE WHEN site_rank > 0 THEN site_rank ELSE NULL END) as control_visits
    #     FROM site_visits
    #     """
    #     results_site_visits = self._execute_query(query)

    #     df = pd.DataFrame(
    #         results_site_visits, columns=["TrainingVisits", "ControlVisits"]
    #     )

    #     # Then we get numbers about the ads

    #     query = """
    #    SELECT
    #         COUNT(DISTINCT ad_url) as num_ads,
    #         COUNT(DISTINCT CASE WHEN va.non_ad  IS NULL AND va.unspecific_ad IS NULL THEN ad_url ELSE NULL END) as filtered_ads,
    # 		COUNT(DISTINCT CASE WHEN (va.non_ad IS NULL AND va.unspecific_ad IS NULL) AND va.categorized = TRUE AND lpc.category_name != "Uncategorized" THEN va.ad_url ELSE NULL END) as categorized_ads,
    # 		COUNT(DISTINCT CASE WHEN (va.non_ad IS NULL AND va.unspecific_ad IS NULL) AND va.categorized = TRUE AND lpc.category_name != "Uncategorized" AND lpc.confident = 1 THEN va.ad_url ELSE NULL END) as confident_ads
    #     FROM visit_advertisements va
    #     LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
    #     LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
    #     """
    #     results_ads = self._execute_query(query)

    #     df = pd.concat(
    #         [
    #             df,
    #             pd.DataFrame(
    #                 results_ads,
    #                 columns=["NumAds", "FilteredAds", "CategorizedAds", "ConfidentAds"],
    #             ),
    #         ],
    #         axis=1,
    #     )

    #     return df

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

    # CREATES A PLOT FOR WITH BOTH TOTAL ADS AND UNIQUE ADS PERCENTAGES
    # def create_plot_for_instance_categories_data(self, data, file_name, data_dir):
    #     df = data

    #     # Sorting the data by 'PercentageFromAllUniqueAds' and 'PercentageFromAllTotalAds' to get rankings
    #     df_sorted_unique = df.sort_values(
    #         by="PercentageFromAllUniqueAds", ascending=False
    #     ).reset_index(drop=True)
    #     df_sorted_total = df.sort_values(
    #         by="PercentageFromAllTotalAds", ascending=False
    #     ).reset_index(drop=True)

    #     # Creating a mapping from category to its rank based on 'NumAds'
    #     rank_map = {
    #         cat: rank for rank, cat in enumerate(df_sorted_unique["Category"], 1)
    #     }

    #     # Selecting the top 15 based on unique ads and adding rank index to the Category label
    #     df_top15 = df_sorted_total.head(15)
    #     df_top15["Category"] = df_top15["Category"].apply(
    #         lambda x: f"{x} ({df_top15[df_top15['Category'] == x].index[0] + 1} / {rank_map[x]})"
    #     )

    #     # Plotting the data
    #     fig, ax = plt.subplots(figsize=(12, 10))

    #     # Bar width
    #     bar_width = 0.4

    #     # Positions of the bars on the x-axis
    #     indices = np.arange(len(df_top15))

    #     if "accept" in file_name:
    #         color = "#228833"
    #     elif "reject" in file_name:
    #         color = "#EE6677"
    #     else:
    #         # yellow
    #         color = "#CCBB44"

    #     # Adding bars for total ads and unique ads percentages
    #     bars_total = ax.bar(
    #         indices - bar_width / 2,
    #         df_top15["PercentageFromAllTotalAds"],
    #         width=bar_width,
    #         color=color,
    #         alpha=1,
    #         label="Total Ads",
    #     )
    #     bars_unique = ax.bar(
    #         indices + bar_width / 2,
    #         df_top15["PercentageFromAllUniqueAds"],
    #         width=bar_width,
    #         color=color,
    #         alpha=0.5,
    #         label="Unique Ads",
    #     )

    #     # Adding labels and title
    #     ax.set_xticks(indices)
    #     ax.set_xticklabels(
    #         df_top15["Category"],
    #         fontsize=14,
    #         rotation=45,
    #         ha="right",
    #         fontweight="bold",
    #     )
    #     ax.set_ylabel("Percentage", fontsize=14, fontweight="bold")
    #     plt.legend(fontsize=12)

    #     # Place the legend in the upper right corner
    #     plt.legend(loc="upper right")

    #     # Annotate percentage values on bars with rotation
    #     for total_bar, unique_bar in zip(bars_total, bars_unique):
    #         total_height = total_bar.get_height()
    #         unique_height = unique_bar.get_height()
    #         label_y_pos_total = total_height + 1  # Slightly offset to avoid overlap
    #         label_y_pos_unique = unique_height + 1
    #         ax.text(
    #             total_bar.get_x() + total_bar.get_width() / 2,
    #             label_y_pos_total,
    #             f"{total_height:.1f}",
    #             ha="center",
    #             va="bottom",
    #             fontsize=12,
    #             color="black",
    #             rotation=45,
    #             fontweight="bold",
    #         )
    #         ax.text(
    #             unique_bar.get_x() + unique_bar.get_width() / 2,
    #             label_y_pos_unique,
    #             f"{unique_height:.1f}",
    #             ha="center",
    #             va="bottom",
    #             fontsize=11,
    #             color="black",
    #             rotation=45,
    #         )

    #     # Customizing x-axis label colors for specific categories and increasing font size
    #     highlight = ["Shopping", "Style & Fashion"]
    #     for label in ax.get_xticklabels():
    #         if "Shopping" in label.get_text() or "Style & Fashion" in label.get_text():
    #             label.set_color("red")

    #     # Adjusting the y-axis to show up to 60%
    #     ax.set_ylim(0, 60)

    #     # Saving the plot as a pdf
    #     plt.savefig(data_dir + "ads_categories/" + f"{file_name}.pdf", bbox_inches="tight")

    def create_plot_for_instance_categories_data(self, data, file_name, data_dir):
        def shorten_name(name):
            if name == "Technology & Computing":
                return "Tech.&Computing"
            elif name == "News / Weather / Information":
                return "News/Weather/Info"
            elif name == "Home & Garden":
                return "Home.&Garden"
            elif name == "Arts & Entertainment":
                return "Arts.&Entertainment"
            elif name == "Non-Standard Content":
                return "Non-Standard"
            elif name == "Hobbies & Interests":
                return "Hobbies.&Interests"
            elif name == "Health & Fitness":
                return "Health.&Fitness"
            elif name == "Uncategorized":
                return "Others"
            else:
                return name

        tier1_categories = []
        for key, value in IAB_CATEGORIES.items():
            tier1_categories.append(value[key])

        df = data

        # Rename "Technology & Computing" to "Tech.&Computing"
        df["Category"] = df["Category"].apply(lambda x: shorten_name(x))

        # Sorting the data by 'PercentageFromAllTotalAds' to get rankings
        df_sorted_total = df.sort_values(
            by="PercentageFromAllTotalAds", ascending=False
        ).reset_index(drop=True)

        # Selecting the top 15 based on total ads and adding rank index to the Category label
        df_top15 = df_sorted_total.head(15)

        # Plotting the data
        fig, ax = plt.subplots(figsize=(15, 12))

        # Bar width
        bar_width = 0.7

        # Positions of the bars on the x-axis
        indices = np.arange(len(df_top15))

        if "accept" in file_name:
            color = "#228833"
            legend = mpatches.Patch(
                facecolor="#228833", edgecolor="black", label="Accept All"
            )
        elif "reject" in file_name:
            color = "#EE6677"
            legend = mpatches.Patch(
                facecolor="#EE6677", edgecolor="black", label="Reject All"
            )
        else:
            # yellow
            color = "#CCBB44"
            legend = mpatches.Patch(
                facecolor="#CCBB44", edgecolor="black", label="No Action"
            )

        # Adding bars for total ads percentages
        bars_total = ax.bar(
            indices,
            df_top15["PercentageFromAllTotalAds"],
            width=bar_width,
            color=color,
            alpha=1,
            label="Total Ads",
        )

        # Adding labels and title
        ax.set_xticks(indices)
        ax.set_xticklabels(
            df_top15["Category"],
            fontsize=36,
            rotation=55,
            ha="right",
        )
        ax.set_ylabel("Percentage", fontsize=36)
        # set the font size of yticks
        ax.tick_params(axis="y", labelsize=28)

        # Add legend
        plt.legend(
            handles=[legend],
            fontsize=35,
            loc="upper right",
        )

        # Annotate percentage values on bars with rotation
        for total_bar in bars_total:
            total_height = total_bar.get_height()
            label_y_pos_total = total_height + 1  # Slightly offset to avoid overlap
            ax.text(
                total_bar.get_x() + total_bar.get_width() / 2,
                label_y_pos_total,
                f"{total_height:.1f}",
                ha="center",
                va="bottom",
                fontsize=26,
                color="black",
            )

        # Customizing x-axis label colors for specific categories and increasing font size
        highlight = ["Shopping", "Style & Fashion"]
        for label in ax.get_xticklabels():
            if "Shopping" in label.get_text() or "Style & Fashion" in label.get_text():
                label.set_color("red")

        # Adjusting the y-axis to show up to 60%
        ax.set_ylim(0, 70)

        print(os.getcwd())

        # Saving the plot as a pdf
        plt.savefig(
            data_dir + f"{file_name}.pdf",
            bbox_inches="tight",
        )

    @staticmethod
    def plot_ad_providers_vs_other(data, file_name, data_dir):
        # Ensure the directory exists
        os.makedirs(os.path.join(data_dir, "boxplots"), exist_ok=True)

        # data = [accept_providers, accept_all, do_nothing_providers, do_nothing_all, reject_providers, reject_all]
        box_colors = ["#228833", "#228833", "#CCBB44", "#CCBB44", "#EE6677", "#EE6677"]
        hatch_patterns = [None, "//", None, "//", None, "//"]

        # Create the plot with the specified size
        plt.figure(figsize=(15, 15))

        # Custom positions for the boxes
        positions = [1, 1.3, 2, 2.3, 3, 3.3]

        # Create the boxplot, hiding outlier points
        boxplot = plt.boxplot(
            data, positions=positions, patch_artist=True, showfliers=False, widths=0.2
        )

        # Customize the colors and hatches of the boxes
        for patch, color, hatch in zip(boxplot["boxes"], box_colors, hatch_patterns):
            patch.set_facecolor(color)
            if hatch is not None:
                patch.set_hatch(hatch)

        # Customize the plot labels
        ylabel_msg = "# Ads"

        plt.ylabel(ylabel_msg, fontsize=35)
        plt.tick_params(axis="y", labelsize=32)
        plt.xticks(
            [1.15, 2.15, 3.15], ["Accept All", "No Action", "Reject All"], fontsize=35
        )

        # Add legend
        no_pattern_patch = mpatches.Patch(
            facecolor="white", edgecolor="black", label="googleadservices.com"
        )
        pattern_patch = mpatches.Patch(
            facecolor="white", edgecolor="black", hatch="//", label="doubleclick.net"
        )
        plt.legend(
            handles=[no_pattern_patch, pattern_patch], fontsize=35, loc="upper right"
        )

        # Reduce spacing between the boxes
        plt.subplots_adjust(left=0.15, right=0.85, top=0.85, bottom=0.15)

        # Save the plot as a PDF file
        plt.savefig(
            os.path.join(data_dir, "boxplots", file_name + ".pdf"), bbox_inches="tight"
        )

        # Close the plot
        plt.close()

    @staticmethod
    def plot_ads_boxplot(data, file_name, data_dir):
        print("DATA = ")
        print(data)

        # Ensure the directory exists
        os.makedirs(os.path.join(data_dir, "boxplots"), exist_ok=True)

        # data = [accept, do_nothing, do_nothing_all, reject]
        box_colors = ["#228833", "#CCBB44", "#EE6677"]

        # Create the plot with the specified size
        plt.figure(figsize=(15, 15))

        # Custom positions for the boxes
        positions = [1, 2, 3]

        # Create the boxplot, hiding outlier points
        boxplot = plt.boxplot(
            data, positions=positions, patch_artist=True, showfliers=False, widths=0.3
        )

        # Customize the colors and hatches of the boxes
        for patch, color in zip(boxplot["boxes"], box_colors):
            patch.set_facecolor(color)

        # Make the median lines thicker, black, and dotted
        for median in boxplot["medians"]:
            median.set_linewidth(3)  # Increase the median line width
            median.set_color("black")  # Change the median line color to black
            median.set_linestyle("--")  # Set the median line style to dotted

        # Customize the plot labels
        ylabel_msg = "# Ads"

        plt.ylabel(ylabel_msg, fontsize=35)
        plt.tick_params(axis="y", labelsize=32)
        plt.xticks([1, 2, 3], ["Accept All", "No Action", "Reject All"], fontsize=35)

        # Reduce spacing between the boxes
        plt.subplots_adjust(left=0.15, right=0.85, top=0.85, bottom=0.15)

        # Save the plot as a PDF file
        plt.savefig(
            os.path.join(data_dir, "boxplots", file_name + ".pdf"), bbox_inches="tight"
        )

        # Close the plot
        plt.close()

    @staticmethod
    def plot_ad_evolution(df_accept, df_ignore, df_reject, file_name, data_dir):
        print("AD ACCEPT:")
        print(df_accept["percentage_by_visit"].T)
        print("AD IGNORE:")
        print(df_ignore["percentage_by_visit"].T)
        print("AD REJECT:")
        print(df_reject["percentage_by_visit"].T)

        sessions = df_accept["session_number"]

        # Create the plot
        fig, ax = plt.subplots(figsize=(15, 9))

        bar_width = 0.3
        opacity = 1

        # Positions for the bars
        index = sessions
        accept_positions = index - bar_width
        ignore_positions = index
        reject_positions = index + bar_width

        # Plot UNIQUE
        # bars_accept_session = ax.bar(
        #     accept_positions,
        #     df_accept["percentage_session"],
        #     bar_width,
        #     alpha=opacity,
        #     label="Accept - Unique",
        #     color="#228833",
        # )
        # bars_ignore_session = ax.bar(
        #     ignore_positions,
        #     df_ignore["percentage_session"],
        #     bar_width,
        #     alpha=opacity,
        #     label="Ignore - Unique",
        #     color="#CCBB44",
        # )
        # bars_reject_session = ax.bar(
        #     reject_positions,
        #     df_reject["percentage_session"],
        #     bar_width,
        #     alpha=opacity,
        #     label="Reject - Unique",
        #     color="#EE6677",
        # )

        # Plot TOTAL
        bars_accept_visit = ax.bar(
            accept_positions,
            df_accept["percentage_by_visit"],
            bar_width,
            alpha=1,
            # alpha=0.5,
            label="Accept All",
            color="#228833",
        )

        bars_ignore_visit = ax.bar(
            ignore_positions,
            df_ignore["percentage_by_visit"],
            bar_width,
            alpha=1,
            # alpha=0.5,
            label="No Action",
            color="#CCBB44",
        )

        bars_reject_visit = ax.bar(
            reject_positions,
            df_reject["percentage_by_visit"],
            bar_width,
            alpha=1,
            # alpha=0.5,
            label="Reject All",
            color="#EE6677",
        )

        # Add labels and title
        ax.set_xlabel("Session Number", fontsize=28)
        ax.set_ylabel("Percentage", fontsize=26)
        ax.tick_params(axis="y", labelsize=26)

        ax.set_xticks(index)
        ax.set_xticklabels(index, fontsize=28)
        ax.legend(fontsize=24)

        # Add value labels on top of each bar
        def add_labels(bars, values):
            for bar, value in zip(bars, values):
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    height,
                    f"{value:.1f}",
                    ha="center",
                    va="bottom",
                    fontsize=24,
                )

        # Add labels UNIQUE
        # add_labels(bars_accept_session, df_accept["percentage_session"])
        # add_labels(bars_ignore_session, df_ignore["percentage_session"])
        # add_labels(bars_reject_session, df_reject["percentage_session"])

        # Add labels TOTAL
        add_labels(bars_accept_visit, df_accept["percentage_by_visit"])
        add_labels(bars_ignore_visit, df_ignore["percentage_by_visit"])
        add_labels(bars_reject_visit, df_reject["percentage_by_visit"])

        # Save the plot as a PDF
        plt.savefig(
            data_dir + "evolution_by_session/" + f"{file_name}.pdf",
            bbox_inches="tight",
        )
        plt.close()

    def close(self):
        """Close the database connection."""
        self.conn.close()

    # def get_ads_by_category_table_all_browsers(self) -> List[Dict]:
    #     """Fetch the number of ads per category for all browsers"""
    #     if not self.conn:
    #         self.connect()
    #     cursor = self.conn.cursor()

    #     query = """
    #             SELECT
    #                 lpc.category_name as category_name,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_1_browser_id THEN va.ad_id ELSE NULL END) as NumAdsURLSession1,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_1_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession1,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_2_browser_id THEN va.ad_id ELSE NULL END) as NumAdsURLSession2,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_2_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession2,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_3_browser_id THEN va.ad_id ELSE NULL END) as NumUniqueAdsURLSession3,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_3_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession3,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_4_browser_id THEN va.ad_id ELSE NULL END) as NumUniqueAdsURLSession4,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_4_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession4,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_5_browser_id THEN va.ad_id ELSE NULL END) as NumUniqueAdsURLSession5,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_5_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession5,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_6_browser_id THEN va.ad_id ELSE NULL END) as NumUniqueAdsURLSession6,
    #                 COUNT(DISTINCT CASE WHEN va.browser_id = :session_6_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession6
    #             FROM visit_advertisements va
    #             LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
    #             LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
    #             WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
    #             GROUP BY lpc.category_name
    #         """
    #     # COUNT(CASE WHEN va.browser_id = :session_7_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession7,
    #     # COUNT(DISTINCT CASE WHEN va.browser_id = :session_7_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession7,
    #     # COUNT(CASE WHEN va.browser_id = :session_8_browser_id THEN va.ad_url ELSE NULL END) as NumAdsURLSession8,
    #     # COUNT(DISTINCT CASE WHEN va.browser_id = :session_8_browser_id THEN va.ad_url ELSE NULL END) as NumUniqueAdsURLSession8
    #     cursor.execute(
    #         query,
    #         {
    #             "session_1_browser_id": self.oba_browsers[0],
    #             "session_2_browser_id": self.oba_browsers[1],
    #             "session_3_browser_id": self.oba_browsers[2],
    #             "session_4_browser_id": self.oba_browsers[3],
    #             "session_5_browser_id": self.oba_browsers[4],
    #             "session_6_browser_id": self.oba_browsers[5],
    #             # "session_7_browser_id": self.oba_browsers[6],
    #             # "session_8_browser_id": self.oba_browsers[7],
    #         },
    #     )
    #     ads = cursor.fetchall()
    #     cursor.close()

    #     ads_df = pd.DataFrame(
    #         ads,
    #         columns=[
    #             "Category",
    #             "NumAdsURLSession1",
    #             "NumUniqueAdsURLSession1",
    #             "NumAdsURLSession2",
    #             "NumUniqueAdsURLSession2",
    #             "NumAdsURLSession3",
    #             "NumUniqueAdsURLSession3",
    #             "NumAdsURLSession4",
    #             "NumUniqueAdsURLSession4",
    #             "NumAdsURLSession5",
    #             "NumUniqueAdsURLSession5",
    #             "NumAdsURLSession6",
    #             "NumUniqueAdsURLSession6",
    #             # "NumAdsURLSession7",
    #             # "NumUniqueAdsURLSession7",
    #             # "NumAdsURLSession8",
    #             # "NumUniqueAdsURLSession8",
    #         ],
    #     )

    #     # Add the total number of ads and unique ads for each category
    #     total_ads = ads_df.filter(like="NumAdsURL").sum(axis=1)
    #     total_unique_ads = ads_df.filter(like="NumUniqueAdsURL").sum(axis=1)

    #     # Insert the total number of ads and unique ads at the beginning of the dataframe
    #     ads_df.insert(1, "TotalNumAdsURL", total_ads)
    #     ads_df.insert(2, "TotalNumUniqueAdsURL", total_unique_ads)

    #     # Sort the rows by the total number of unique ads
    #     ads_df = ads_df.sort_values(by="TotalNumAdsURL", ascending=False)

    #     # Add a new column with a boolean indicating if the category is tier 1 or not
    #     tier1_categories = []
    #     for key, value in IAB_CATEGORIES.items():
    #         tier1_categories.append(value[key])

    #     ads_df.insert(1, "IsTier1", ads_df["Category"].isin(tier1_categories))

    #     print(ads_df[ads_df["IsTier1"]])

    #     # Print the total number of ads in total
    #     print(ads_df["TotalNumAdsURL"].sum())
    #     print(ads_df["TotalNumUniqueAdsURL"].sum())

    #     return ads_df

    # def get_top_10_domains_ads(self) -> List[Dict]:
    #     if not self.conn:
    #         self.connect()
    #     cursor = self.conn.cursor()

    #     query = """
    #             SELECT
    #                 va.ad_url,
    #                 browser_id
    #             FROM visit_advertisements va
    #             WHERE va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL AND va.browser_id != :session_8_browser_id AND va.browser_id != :session_7_browser_id
    #         """
    #     cursor.execute(
    #         query,
    #         {
    #             "session_7_browser_id": self.oba_browsers[6],
    #             "session_8_browser_id": self.oba_browsers[7],
    #         },
    #     )
    #     ads = cursor.fetchall()
    #     cursor.close()

    #     # Create a DataFrame with the ads
    #     df = pd.DataFrame(ads, columns=["AdURL", "Session"])

    #     df["Domain"] = df["AdURL"].apply(
    #         lambda x: tldextract.extract(x).registered_domain
    #     )

    #     # Group by 'Domain' and aggregate
    #     domain_summary = (
    #         df.groupby("Domain")
    #         .agg(
    #             Total_AdURLs=("AdURL", "count"),  # Count total AdURLs per domain
    #             Unique_AdURLs=(
    #                 "AdURL",
    #                 pd.Series.nunique,
    #             ),  # Count unique AdURLs per domain
    #         )
    #         .reset_index()
    #     )  # Reset index to make 'Domain' a column

    #     # Sort by 'Unique_AdURLs' in descending order
    #     domain_summary = domain_summary.sort_values(by="Unique_AdURLs", ascending=False)

    #     # Print the total number of ads in total
    #     print(domain_summary["Total_AdURLs"].sum())
    #     print(domain_summary["Unique_AdURLs"].sum())

    #     # Print all the domains as a list of strings
    #     print(domain_summary["Domain"].to_list())

    #     # Write the results to a markdown table file
    #     domain_summary.to_markdown(
    #         f"{self.experiment_dir}/results/top_domains_ads.md", index=False
    #     )

    #     # Create a new dataframe whose rows are the different groups of domains and the columns are the number of ads and unique ads for each group
    #     domain_groups = {
    #         "Nothing": NOTHING_GROUP,
    #         "A": A_GROUP,
    #         "M": M_GROUP,
    #         "M-": M_MINUS_GROUP,
    #         "U": U_GROUP,
    #     }

    #     # Create a new dataframe with the domain groups
    #     df_domain_groups = pd.DataFrame(columns=["Group", "TotalAds", "TotalUniqueAds"])

    #     for group, domains in domain_groups.items():
    #         df_group = domain_summary[domain_summary["Domain"].isin(domains)]
    #         total_ads = df_group["Total_AdURLs"].sum()
    #         total_unique_ads = df_group["Unique_AdURLs"].sum()
    #         row = pd.DataFrame(
    #             [
    #                 {
    #                     "Group": group,
    #                     "TotalAds": total_ads,
    #                     "TotalUniqueAds": total_unique_ads,
    #                 }
    #             ]
    #         )
    #         df_domain_groups = pd.concat([df_domain_groups, row])

    #     # Write the results to a markdown table file
    #     df_domain_groups.to_markdown(
    #         f"{self.experiment_dir}/results/domain_groups_ads.md", index=False
    #     )

    #     print(df_domain_groups)

    #     return

    # def get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
    #     self,
    #     numvisits_by_browser_id_and_url: List[Tuple[int, str, int]],
    # ) -> List[Dict]:
    #     """Given a list of tuples where the first element is the browser_id, the second is the site_url and the third is the number of visits, return the number of ad_urls and the number of distinct ad_urls whose landing page was categorized with the given category for each session."""

    #     def fetch_ads_by_limits_and_site_url_and_category(
    #         cursor,
    #         site_url: str,
    #         num_visits: int,
    #         already_taken: int,
    #         category_name: str,
    #     ):
    #         # print(
    #         #     f"Fetching ads for {site_url} with {num_visits} visits. Already taken: {already_taken} visits from this site."
    #         # )

    #         query = """
    #             WITH VisitAdCounts AS (
    #                 SELECT
    #                     sv.site_rank,
    #                     COUNT(CASE WHEN lpc.category_name = :category_name THEN va.ad_id ELSE NULL END) AS NumAdsURL,
    #                     COUNT(DISTINCT CASE WHEN lpc.category_name = :category_name THEN va.ad_url ELSE NULL END) AS NumUniqueAdsURL
    # 				FROM site_visits sv
    #                 LEFT JOIN visit_advertisements va ON sv.visit_id = va.visit_id
    #                 LEFT JOIN landing_pages lp ON va.landing_page_id = lp.landing_page_id
    #                 LEFT JOIN landing_page_categories lpc ON lp.landing_page_id = lpc.landing_page_id
    #                 WHERE sv.site_url = :site_url AND va.categorized = TRUE AND va.non_ad IS NULL AND va.unspecific_ad IS NULL
    #                 GROUP BY sv.site_rank
    #             ),
    #             OrderedVisits AS (
    #                 SELECT *,
    #                     ROW_NUMBER() OVER (ORDER BY site_rank) AS rownum
    #                 FROM VisitAdCounts
    #             )
    #             SELECT SUM(NumAdsURL), SUM(NumUniqueAdsURL)
    #             FROM OrderedVisits
    #             WHERE rownum > :already_taken AND rownum <= :already_taken + :num_visits
    #             ORDER BY site_rank;
    #             """

    #         data = {
    #             "category_name": category_name,
    #             "control_run": 1 if self.control_runs else 0,
    #             "site_url": site_url,
    #             "num_visits": num_visits,
    #             "already_taken": already_taken,
    #         }

    #         cursor.execute(query, data)

    #         # print(query, data)

    #         result = cursor.fetchall()
    #         return result

    #     if not self.conn:
    #         self.connect()
    #     cursor = self.conn.cursor()

    #     # Fetch all the category names found in the database
    #     query = """
    #         SELECT DISTINCT category_name
    #         FROM landing_page_categories
    #         """
    #     cursor.execute(query)
    #     categories = cursor.fetchall()
    #     categories = [category[0] for category in categories]

    #     # Initialize the sessions list and the set of seen browser_ids
    #     site_visits_count = {
    #         site_url: 0 for _, site_url, _ in numvisits_by_browser_id_and_url
    #     }

    #     # Create a dictionary with the number of ads and unique ads for each session for each category
    #     categories_ads = {}
    #     for category in categories:
    #         categories_ads[category] = {}
    #         for i in range(1, 9):
    #             categories_ads[category][f"NumAdsURL_Session{i}"] = 0
    #             categories_ads[category][f"NumUniqueAdsURL_Session{i}"] = 0

    #     session_number = 0  # Initialize the session number
    #     seen_browser_ids = set()
    #     for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
    #         if browser_id not in seen_browser_ids:
    #             # Move to the next session
    #             session_number += 1

    #             # Add the session to seen sessions
    #             seen_browser_ids.add(browser_id)

    #         if session_number == 7:
    #             # We only want to analyze until session 6
    #             break

    #         for category in categories:
    #             # Fetch the ads from num_visits visits to the current site_url, starting from the first visit after the already taken visit
    #             query_result = fetch_ads_by_limits_and_site_url_and_category(
    #                 cursor,
    #                 site_url,
    #                 num_visits,
    #                 site_visits_count[site_url],
    #                 category,
    #             )

    #             # Add the number of ads and unique ads to the corresponding category
    #             numAdsURL, numUniqueAdsURL = query_result[0]

    #             if numAdsURL is None:
    #                 numAdsURL = 0
    #             if numUniqueAdsURL is None:
    #                 numUniqueAdsURL = 0
    #             key_num_ads_url = f"NumAdsURL_Session{session_number}"
    #             key_num_unique_ads_url = f"NumUniqueAdsURL_Session{session_number}"

    #             categories_ads[category][key_num_ads_url] += numAdsURL
    #             categories_ads[category][key_num_unique_ads_url] += numUniqueAdsURL

    #         # Update the number of visits taken from the site_url
    #         site_visits_count[site_url] += num_visits

    #     cursor.close()

    #     # With the dictionary of categories_ads, we can now create a DataFrame where each row is a category and each column is the number of ads and unique ads for each session
    #     df = pd.DataFrame(categories_ads).T

    #     # Name the index
    #     df.index.name = "Category"

    #     # Add the total number of ads and unique ads for each category
    #     total_ads = df.filter(like="NumAdsURL").sum(axis=1)
    #     total_unique_ads = df.filter(like="NumUniqueAdsURL").sum(axis=1)

    #     # Insert the total number of ads and unique ads at the beginning of the dataframe
    #     df.insert(0, "TotalNumAdsURL", total_ads)
    #     df.insert(1, "TotalNumUniqueAdsURL", total_unique_ads)

    #     # Sort the rows by the total number of unique ads
    #     df = df.sort_values(by="TotalNumUniqueAdsURL", ascending=False)

    #     print(df)

    #     # Add a new column with a boolean indicating if the category is tier 1 or not
    #     tier1_categories = []
    #     for key, value in IAB_CATEGORIES.items():
    #         tier1_categories.append(value[key])

    #     df.insert(0, "IsTier1", df.index.isin(tier1_categories))

    #     return df
