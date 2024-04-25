from datetime import datetime
import json
import os
import sqlite3
import pandas as pd
from adblockparser import AdblockRules
import time
from tqdm import tqdm
import tldextract
import csv
from oba.experiment_metrics import ExperimentMetrics

# CONTROL_RUN
EXPERIMENT_DIR = "/Volumes/LaCie/OpenOBA/control_runs/"
experiment_name = "control_run_accept"
db_path = f"{EXPERIMENT_DIR}/{experiment_name}/crawl-data.sqlite"
csv_file = f"{EXPERIMENT_DIR}/{experiment_name}/results/cookies_host_check.csv"
markdown_file = (
    f"{EXPERIMENT_DIR}/{experiment_name}/results/cookies_third_party_metrics.md"
)

# OBA_RUN
OBA_DATA_DIR = "/Volumes/LaCie/OpenOBA/oba_runs/"
oba_experiment_name = "style_and_fashion_experiment_accept"
oba_db_path = f"{OBA_DATA_DIR}/{oba_experiment_name}/crawl-data-copy.sqlite"
oba_experiment_json = (
    f"{OBA_DATA_DIR}/{oba_experiment_name}/{oba_experiment_name}_config.json"
)
# Get browser_ids from the JSON file
with open(oba_experiment_json, "r") as f:
    experiment_config = json.load(f)
    oba_browser_ids_order = experiment_config["browser_ids"]["oba"]


def print_timestamped_message(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}")


def fetch_entries_by_limits_and_site_url(
    cursor,
    site_url: str,
    num_visits: int,
    already_taken: int,
    browser_id: int,
):
    # print(
    #     f"Fetching ads for {site_url} with {num_visits} visits. Already taken: {already_taken} visits from this site."
    # )

    query = """
    WITH RankedVisits AS (
        SELECT
            sv.visit_id as visit_id,
            sv.site_rank as site_rank,
            sv.site_url as site_url,
            ROW_NUMBER() OVER (ORDER BY sv.site_rank) AS visit_order
        FROM site_visits sv
        WHERE sv.site_url = :site_url
    ),
    FilteredVisits AS (
        SELECT *
        FROM RankedVisits
        WHERE visit_order BETWEEN :already_taken + 1 AND :already_taken + :num_visits
    )
    SELECT c.id AS id, fv.site_rank, fv.site_url, fv.visit_order, :browser_id AS browser_id
    FROM javascript_cookies c
    JOIN FilteredVisits fv ON c.visit_id = fv.visit_id
    """

    data = {
        "site_url": site_url,
        "num_visits": num_visits,
        "already_taken": already_taken,
        "browser_id": browser_id,
    }

    cursor.execute(query, data)

    # print(query, data)
    df = pd.DataFrame(
        cursor.fetchall(),
        columns=["id", "site_rank", "site_url", "visit_order", "browser_id"],
    )

    return df


# First we need to connect the database
conn = sqlite3.connect(oba_db_path)

# Open files to get the block lists

# First get the actual absolute path of the script
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)


with open(f"{script_dir}/tracking_lists/easyprivacy-justdomains.txt") as f:
    easyprivacy_list = f.read().splitlines()
with open(f"{script_dir}/tracking_lists/easylist-justdomains.txt") as f:
    easylist_list = f.read().splitlines()
with open(f"{script_dir}/tracking_lists/adserverlist-justdomains.txt") as f:
    adserverlist_list = f.read().splitlines()

# Query all the cookies from the database with host ordered by id
query = """
    SELECT c.id as id,
        c.host as host,
        c.name as name,
        c.value as value,
        sv.site_url as visit_url
    FROM javascript_cookies c
    JOIN site_visits sv ON c.visit_id = sv.visit_id
    ORDER BY c.id
    """

# Execute the query
cursor = conn.cursor()
cursor.execute(query)

# Get all the cookies to a DataFrame
df_cookies = pd.DataFrame(
    cursor.fetchall(),
    columns=["id", "host", "name", "value", "visit_url"],
)

# Create a set with the unique hosts
# Add the host_domain and visit_url_domain to the DataFrame
df_cookies["host_domain"] = df_cookies["host"].apply(
    lambda x: tldextract.extract(x).registered_domain
)
unique_hosts_domain = set(df_cookies["host_domain"])
block_results = {}

# Evaluate the rules for each host
start_time = time.time()
print(
    f"Starting Tracking Evaluation processing for {len(unique_hosts_domain)} hosts..."
)
for host_domain in tqdm(unique_hosts_domain):
    # easyprivacy_result = easyprivacy_rules.should_block(host, rule_params)
    # easylist_result = easylist_rules.should_block(host, rule_params)
    # adserverlist_result = adserverlist_rules.should_block(host, rule_params)
    # block_results[host] = [easyprivacy_result, easylist_result, adserverlist_result]

    block_results[host_domain] = [
        host_domain in easyprivacy_list,
        host_domain in easylist_list,
        host_domain in adserverlist_list,
    ]
end_time = time.time()


print(
    f"Finished evaluating all unique HOSTS after {end_time - start_time:.2f} seconds."
)


df_cookies["easyprivacy"] = df_cookies["host_domain"].apply(
    lambda x: block_results[x][0]
)
df_cookies["easylist"] = df_cookies["host_domain"].apply(lambda x: block_results[x][1])
df_cookies["adserverlist"] = df_cookies["host_domain"].apply(
    lambda x: block_results[x][2]
)

# Check for third-party cookies
df_cookies["visit_url_domain"] = df_cookies["visit_url"].apply(
    lambda x: tldextract.extract(x).registered_domain
)

df_cookies["third_party"] = df_cookies["host_domain"] != df_cookies["visit_url_domain"]

# Write the DataFrame to a CSV file
df_cookies.to_csv(csv_file, index=False)

end_processing_time = time.time()
print_timestamped_message(
    f"All URLs processed. Total runtime: {end_processing_time - start_time:.2f} seconds."
)

# Get the number of visits by browser_id and url in the OBA experiment
oba_experiment_metrics = ExperimentMetrics(oba_experiment_name)
numvisits_by_browser_id_and_url = (
    oba_experiment_metrics.get_control_visits_by_url_and_browser()
)

print_timestamped_message(numvisits_by_browser_id_and_url)

# Initialize the sessions list and the set of seen browser_ids
site_visits_count = {site_url: 0 for _, site_url, _ in numvisits_by_browser_id_and_url}

session_number = 0  # Initialize the session number

cursor = conn.cursor()

df_cookies_2 = pd.DataFrame(
    columns=["id", "site_rank", "site_url", "visit_order", "browser_id"]
)  # Initialize the DataFrame to store the cookies data

for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
    # Fetch the ads from num_visits visits to the current site_url, starting from the first visit after the already taken visit
    query_result_df = fetch_entries_by_limits_and_site_url(
        cursor, site_url, num_visits, site_visits_count[site_url], browser_id
    )

    # Add the fetched data to the DataFrame
    df_cookies_2 = pd.concat([df_cookies_2, query_result_df])

    # Update the number of visits taken from the site_url
    site_visits_count[site_url] += num_visits

cursor.close()

# Order df by id
df_cookies_2 = df_cookies_2.sort_values("id").reset_index(drop=True)

print_timestamped_message(df_cookies_2)

print_timestamped_message(f"Query completed. Retrieved {len(df_cookies_2)} rows.")

print_timestamped_message("Merging DataFrames...")

# Assuming the row order in the CSV and queried DataFrame matches exactly
# Add new columns to the CSV df with the browser_id
df_cookies.insert(1, "browser_id", df_cookies_2["browser_id"])

# Perform groupby and aggregate operations
grouped_df = df_cookies.groupby("browser_id")

# Create a new DataFrame to store results
results_table = pd.DataFrame(
    {
        # Unique combinations of host-name-value
        "NumUniqueCookies": grouped_df.apply(
            lambda x: x.drop_duplicates(subset=["host", "name", "value"]).shape[0]
        ),
        # Unique combinations of host-name-value with third-party cookies
        "NumUniqueCookies_3rdParty": grouped_df.apply(
            lambda x: x[x["third_party"]]
            .drop_duplicates(subset=["host", "name", "value"])
            .shape[0]
        ),
        # Unique combinations of host-name-value with third-party cookies and any filter list true
        "NumUniqueCookies_3rdParty_Tracking": grouped_df.apply(
            lambda x: x[
                x["easyprivacy"] | x["easylist"] | x["adserverlist"] & x["third_party"]
            ]
            .drop_duplicates(subset=["host", "name", "value"])
            .shape[0]
        ),
        "NumUniqueCookiesDomains": grouped_df.apply(
            lambda x: x.drop_duplicates(
                subset=["host_domain", "host", "name", "value"]
            ).shape[0]
        ),
        "NumUniqueCookiesDomains_3rdParty": grouped_df.apply(
            lambda x: x[x["third_party"]]
            .drop_duplicates(subset=["host_domain", "host", "name", "value"])
            .shape[0]
        ),
        "NumUniqueCookiesDomains_3rdParty_Tracking": grouped_df.apply(
            lambda x: x[
                x["easyprivacy"] | x["easylist"] | x["adserverlist"] & x["third_party"]
            ]
            .drop_duplicates(subset=["host_domain", "host", "name", "value"])
            .shape[0]
        ),
    }
)

# Reset index if necessary to make 'browser_id' a column again
results_table.reset_index(inplace=True)

results_table["browser_id"] = pd.Categorical(
    results_table["browser_id"], categories=oba_browser_ids_order, ordered=True
)

results_table = results_table.sort_values("browser_id")

# Exclude row with browser_id = 0
results_table = results_table[
    results_table["browser_id"].isin(oba_browser_ids_order)
].reset_index(drop=True)

# From results_table, get the total number of cookies for the first 6 rows
first_6_rows = df_cookies[df_cookies["browser_id"].isin(oba_browser_ids_order[:6])]
summary_data = {
    "browser_id": "-",
    "NumUniqueCookies": first_6_rows.drop_duplicates(
        subset=["host", "name", "value"]
    ).shape[0],
    "NumUniqueCookies_3rdParty": first_6_rows[first_6_rows["third_party"]]
    .drop_duplicates(subset=["host", "name", "value"])
    .shape[0],
    "NumUniqueCookies_3rdParty_Tracking": first_6_rows[
        first_6_rows["third_party"]
        & (
            first_6_rows["easyprivacy"]
            | first_6_rows["easylist"]
            | first_6_rows["adserverlist"]
        )
    ]
    .drop_duplicates(subset=["host", "name", "value"])
    .shape[0],
    "NumUniqueCookiesDomains": first_6_rows.drop_duplicates(
        subset=["host_domain", "host", "name", "value"]
    ).shape[0],
    "NumUniqueCookiesDomains_3rdParty": first_6_rows[first_6_rows["third_party"]]
    .drop_duplicates(subset=["host_domain", "host", "name", "value"])
    .shape[0],
    "NumUniqueCookiesDomains_3rdParty_Tracking": first_6_rows[
        first_6_rows["third_party"]
        & (
            first_6_rows["easyprivacy"]
            | first_6_rows["easylist"]
            | first_6_rows["adserverlist"]
        )
    ]
    .drop_duplicates(subset=["host_domain", "host", "name", "value"])
    .shape[0],
}
# Convert to DataFrame for a single row result
summary_row = pd.DataFrame([summary_data], index=["Combined First 6 Sessions"])

# Append this summary row to the results_table
results_table = pd.concat([results_table, summary_row])

print(results_table)

results_table_markdown = results_table.to_markdown()

# # To save to a Markdown file
with open(markdown_file, "w") as f:
    f.write("## Cookies Metrics\n")
    f.write(results_table_markdown)

print(f"FINISHED FOR {experiment_name}!")
