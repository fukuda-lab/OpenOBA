from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3
import json

DATA_DIR = "/Volumes/LaCie/OpenOBA/oba_runs/"
RESULTS_DIR = "/Volumes/LaCie/OpenOBA/RESULTS/"


def print_with_timestamp(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def load_csv_and_sqlite_to_df(experiment_name):
    print_with_timestamp(f"---- Starting to load data from {experiment_name} ----")

    csv_path = (
        f"{DATA_DIR}/{experiment_name}/results/http_requests_url_third_parties.csv"
    )
    sqlite_path = f"{DATA_DIR}/{experiment_name}/crawl-data-copy.sqlite"

    # Load data from CSV to DataFrame
    print_with_timestamp(f"Loading CSV...")
    df = pd.read_csv(csv_path)

    # Open sqlite and read data to DataFrame
    conn = sqlite3.connect(sqlite_path)
    print_with_timestamp(f"Loading SQLite...")
    df_sqlite = pd.read_sql_query(
        f"SELECT id, browser_id FROM http_requests ORDER BY id", conn
    )
    # Close connection
    conn.close()

    # Aggregate browser_id column from SQLite to CSV
    df.insert(1, "browser_id", df_sqlite["browser_id"])

    # Read json with browser_ids_ordered
    json_path = f"{DATA_DIR}/{experiment_name}/{experiment_name}_config.json"

    # Get browser_ids from the JSON file
    with open(json_path, "r") as f:
        experiment_config = json.load(f)
        browser_ids_ordered = experiment_config["browser_ids"]["oba"]
        browser_ids_ordered = browser_ids_ordered[:6]

    print_with_timestamp(f"Organizing experiment sessions...")

    # Group by browser_id, keep only the ones in browser_ids_ordered, and add a column with the order, then sort by order
    df_grouped = df.groupby("browser_id").filter(
        lambda x: x.name in browser_ids_ordered
    )
    df_grouped.insert(
        0,
        "session",
        df_grouped["browser_id"].apply(lambda x: browser_ids_ordered.index(x) + 1),
    )
    df_grouped = df_grouped.sort_values(by="session")

    # Filter only third party tracking domains per session
    df_grouped = df_grouped[
        (df_grouped["third_party"])
        & (
            df_grouped["easyprivacy"]
            | df_grouped["easylist"]
            | df_grouped["adserverlist"]
        )
    ]

    # Reset index
    df_grouped = df_grouped.reset_index(drop=True)

    # Now we need to leave the DataFrame as ["session", "domain"]
    df_grouped = df_grouped[["session", "request_url_domain"]]

    # Group by session and leave only unique domains
    df_grouped = (
        df_grouped.groupby("session")["request_url_domain"].unique().reset_index()
    )

    # Rename request_url_domain to domains
    df_grouped = df_grouped.rename(columns={"request_url_domain": "domains"})

    # Add a row with the domains present in all sessions
    df_grouped = df_grouped.append(
        {
            "session": "all",
            "domains": set.union(*df_grouped["domains"].apply(set).values),
        },
        ignore_index=True,
    )

    return df_grouped


def get_domains_present_in_three_instances_by_session(
    df_accept, df_do_nothing, df_reject
):
    # We have three DataFrames with the domains present in each session
    # We need to find the domains present in all three instances for each session
    # We will return a DataFrame with the session and the domains present in all three instances

    # Merge the DataFrames
    df = df_accept.merge(
        df_do_nothing, on="session", suffixes=("_accept", "_do_nothing")
    )
    print(df)
    df = df.merge(df_reject, on="session")

    # Rename the domains column to domains_reject
    df = df.rename(columns={"domains": "domains_reject"})

    # Create a new column with the domains present in all three instances
    df["domains_overlap"] = df.apply(
        lambda x: set(x["domains_accept"])
        & set(x["domains_do_nothing"])
        & set(x["domains_reject"]),  # Reject does not have a suffix
        axis=1,
    )

    print(df)

    return df


# Load data for the experiment
df_accept = load_csv_and_sqlite_to_df("style_and_fashion_experiment_accept")
print(df_accept)
df_do_nothing = load_csv_and_sqlite_to_df("style_and_fashion_experiment_do_nothing")
print(df_do_nothing)
df_reject = load_csv_and_sqlite_to_df("style_and_fashion_experiment_reject")
print(df_reject)

# Get the domains present in all three instances by session
df_overlaps = get_domains_present_in_three_instances_by_session(
    df_accept, df_do_nothing, df_reject
)

print(df_overlaps)

# Create a new DataFrame with the session and the number of domains present in each instance and in all three instances. It must be a new DataFrame with the columns: session, num_domains_accept, num_domains_do_nothing, num_domains_reject, num_domains_overlap
df_num_domains = pd.DataFrame()
df_num_domains["session"] = df_overlaps["session"]
df_num_domains["num_domains_accept"] = df_overlaps["domains_accept"].apply(len)
df_num_domains["num_domains_do_nothing"] = df_overlaps["domains_do_nothing"].apply(len)
df_num_domains["num_domains_reject"] = df_overlaps["domains_reject"].apply(len)
df_num_domains["num_domains_overlap"] = df_overlaps["domains_overlap"].apply(len)

print(df_num_domains)

# Save the DataFrame to a Markdown table
df_num_domains.to_markdown(
    f"{RESULTS_DIR}/http_domains_by_session_all_instances.md", index=False
)
