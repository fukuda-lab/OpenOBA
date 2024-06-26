import json
import pandas as pd
import sqlite3
from datetime import datetime


def print_timestamped_message(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}")


DATA_DIR = "/Volumes/LaCie/OpenOBA/oba_runs/"
experiment_name = "style_and_fashion_experiment_do_nothing"
db_path = f"{DATA_DIR}/{experiment_name}/crawl-data-copy.sqlite"
csv_path = f"{DATA_DIR}/{experiment_name}/results/http_requests_url_third_parties.csv"
experiment_json = f"{DATA_DIR}/{experiment_name}/{experiment_name}_config.json"
# Get browser_ids from the JSON file
with open(experiment_json, "r") as f:
    experiment_config = json.load(f)
    browser_ids_order = experiment_config["browser_ids"]["oba"]

print(browser_ids_order)


markdown_file = (
    f"{DATA_DIR}/{experiment_name}/results/http_requests_third_party_metrics.md"
)

# Connect to the SQLite database
print_timestamped_message(f"Script started for {experiment_name}...")
print_timestamped_message("Connecting to database...")
conn = sqlite3.connect(db_path)

# Query to get the url from http_requests
print_timestamped_message("Querying SQLite database for browser_id information...")
query = """
SELECT id as id,
       browser_id as browser_id
FROM http_requests
ORDER BY id
"""
df_http_requests_browser_id = pd.read_sql_query(query, conn)

print_timestamped_message(
    f"Query completed. Retrieved {len(df_http_requests_browser_id)} rows."
)

# Load the CSV file
print_timestamped_message("Loading CSV...")
csv_df = pd.read_csv(csv_path)


def aggregate_data(group, summary=False):
    data = {
        "NumUniqueURL": group["url"].nunique(),
        "NumUniqueURL_3rdParty": group[group["third_party"]]["url"].nunique(),
        "NumUniqueURL_3rdParty_Tracking": group[
            (group["third_party"])
            & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
        ]["url"].nunique(),
        "NumUniqueDomain_3rdParty": group[group["third_party"]][
            "request_url_domain"
        ].nunique(),
        "NumUniqueDomain_3rdParty_Tracking": group[
            (group["third_party"])
            & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
        ]["request_url_domain"].nunique(),
    }

    if summary:
        data["browser_id"] = "-"

    return pd.Series(data)


def get_domains_data(group):
    # Step 1: Further filter to get only 3rd party tracking entries
    tracking_df = group[
        group["third_party"]
        & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
    ]

    # Step 2: Collect sets of domains for each session
    domain_sets = tracking_df.groupby("browser_id")["request_url_domain"].apply(set)

    # Step 3: Find the intersection of all domain sets
    common_domains = (
        set.intersection(*domain_sets.values) if len(domain_sets) > 0 else set()
    )

    # Step 3: Count occurrences of each domain
    domain_counts = tracking_df["request_url_domain"].value_counts()

    # Make a DataFrame with the domain, the count, and the percentage of the total requests
    domain_counts = pd.DataFrame(domain_counts).reset_index()
    domain_counts.columns = ["Domain", "Count"]
    domain_counts["Percentage"] = domain_counts["Count"] / len(tracking_df) * 100
    # Add a column with the cumulative percentage
    domain_counts["Cumulative Percentage"] = domain_counts["Percentage"].cumsum()

    # Step 4: Get the top 10 domains
    # top_10_domains = domain_counts.head(10)

    return domain_counts, len(common_domains)


def aggregate_data_control(group, summary=False):
    data = {
        "Control_NumUniqueURL": group[group["control_visit"]]["url"].nunique(),
        "Control_NumUniqueURL_3rdParty": group[
            group["control_visit"] & group["third_party"]
        ]["url"].nunique(),
        "Control_NumUniqueURL_3rdParty_Tracking": group[
            (group["control_visit"])
            & (group["third_party"])
            & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
        ]["url"].nunique(),
        "Control_NumUniqueDomain_3rdParty": group[
            group["control_visit"] & group["third_party"]
        ]["request_url_domain"].nunique(),
        "Control_NumUniqueDomain_3rdParty_Tracking": group[
            (group["control_visit"])
            & (group["third_party"])
            & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
        ]["request_url_domain"].nunique(),
    }

    if summary:
        data["browser_id"] = "-"
    return pd.Series(data)


# Ensure the DataFrame from the database has the same number of rows as the CSV DataFrame
if len(df_http_requests_browser_id) == len(csv_df):
    print_timestamped_message("Merging DataFrames...")

    # Assuming the row order in the CSV and queried DataFrame matches exactly
    # Add new columns to the CSV df with the browser_id
    csv_df.insert(1, "browser_id", df_http_requests_browser_id["browser_id"])

    # Perform groupby and aggregate operations
    aggregated_data = csv_df.groupby("browser_id").apply(aggregate_data)

    # Reset index if necessary to make 'browser_id' a column again
    aggregated_data.reset_index(inplace=True)

    aggregated_data["browser_id"] = pd.Categorical(
        aggregated_data["browser_id"], categories=browser_ids_order, ordered=True
    )

    aggregated_data = aggregated_data.sort_values("browser_id")

    # Exclude row with browser_id = 0
    aggregated_data = aggregated_data[
        aggregated_data["browser_id"].isin(browser_ids_order)
    ].reset_index(drop=True)

    # Obtain numbers for the combined first 6 sessions
    summary_data = aggregate_data(
        csv_df[csv_df["browser_id"].isin(browser_ids_order[:6])], summary=True
    )
    # Convert to DataFrame for a single row result
    summary_row = pd.DataFrame([summary_data], index=["Combined First 6 Sessions"])
    # Append this summary row to the results_table
    results_table = pd.concat([aggregated_data, summary_row])

    # OBTAIN THE TOP 10 DOMAINS AND THE OVERLAP OF 3RD PARTY TRACKING DOMAINS IN THE FIRST 6 SESSIONS
    top_10_domains, common_domains = get_domains_data(
        csv_df[csv_df["browser_id"].isin(browser_ids_order[:6])]
    )
    # Create a row with the same shape as the results_table but with - for all values except the NumUniqueDomain_3rdParty_Tracking column which will contain the number of common domains
    common_domains_row = pd.DataFrame(
        [{"browser_id": "-", "NumUniqueDomain_3rdParty_Tracking": common_domains}],
        index=["Common 3rd Party Tracking Domains in First 6 Sessions"],
    )

    # Append this row to the results_table
    results_table = pd.concat([results_table, common_domains_row])
    results_table_markdown = results_table.to_markdown()

    # Now repeat everything but considering an additional 'control_visit' filter = True for the 4 previous column
    aggregated_data_control = csv_df.groupby("browser_id").apply(aggregate_data_control)

    # Reset index if necessary to make 'browser_id' a column again
    aggregated_data_control.reset_index(inplace=True)

    aggregated_data_control["browser_id"] = pd.Categorical(
        aggregated_data_control["browser_id"],
        categories=browser_ids_order,
        ordered=True,
    )

    aggregated_data_control = aggregated_data_control.sort_values("browser_id")

    # Exclude row with browser_id = 0
    aggregated_data_control = aggregated_data_control[
        aggregated_data_control["browser_id"].isin(browser_ids_order)
    ].reset_index(drop=True)

    # Obtain numbers for the combined first 6 sessions
    summary_data_control = aggregate_data_control(
        csv_df[csv_df["browser_id"].isin(browser_ids_order[:6])], summary=True
    )
    # Convert to DataFrame for a single row result
    summary_row_control = pd.DataFrame(
        [summary_data_control], index=["Combined First 6 Sessions"]
    )

    # Append this summary row to the results_table
    results_table_control = pd.concat([aggregated_data_control, summary_row_control])

    # OBTAIN THE OVERLAP OF 3RD PARTY TRACKING DOMAINS IN THE FIRST 6 SESSIONS
    top_10_domains_control, common_domains_control = get_domains_data(
        csv_df[
            csv_df["browser_id"].isin(browser_ids_order[:6]) & csv_df["control_visit"]
        ]
    )
    # Create a row with the same shape as the results_table but with - for all values except the Control_NumUniqueDomain_3rdParty_Tracking column which will contain the number of common domains
    common_domains_control_row = pd.DataFrame(
        [
            {
                "browser_id": "-",
                "Control_NumUniqueDomain_3rdParty_Tracking": common_domains_control,
            }
        ],
        index=["Common 3rd Party Tracking Domains in First 6 Sessions"],
    )

    # Append this row to the results_table_control
    results_table_control = pd.concat(
        [results_table_control, common_domains_control_row]
    )

    control_results_table_markdown = results_table_control.to_markdown()

    # Now, in a csv file, list all the unique tracking, third-party domains for each browser_id along with the number of requests to each domain
    # This will be useful for the next step of the analysis
    csv_unique_tracking_domains = (
        f"{DATA_DIR}/{experiment_name}/results/unique_tracking_3rd_party_domains.csv"
    )
    csv_unique_tracking_domains_control = f"{DATA_DIR}/{experiment_name}/results/unique_tracking_3rd_party_control_domains.csv"

    # Get the unique tracking domains for each browser_id
    unique_tracking_domains = (
        csv_df[
            csv_df["third_party"]
            & (csv_df["easyprivacy"] | csv_df["easylist"] | csv_df["adserverlist"])
        ]
        .groupby("browser_id")["request_url_domain"]
        .value_counts()
        .unstack(fill_value=0)
    )

    # Get the unique tracking domains for each browser_id with control_visit = True
    unique_tracking_domains_control = (
        csv_df[
            csv_df["control_visit"]
            & csv_df["third_party"]
            & (csv_df["easyprivacy"] | csv_df["easylist"] | csv_df["adserverlist"])
        ]
        .groupby("browser_id")["request_url_domain"]
        .value_counts()
        .unstack(fill_value=0)
    )

    # Save to CSV
    unique_tracking_domains.transpose().to_csv(csv_unique_tracking_domains)
    unique_tracking_domains_control.transpose().to_csv(
        csv_unique_tracking_domains_control
    )

    print(f"Unique tracking domains saved to {csv_unique_tracking_domains}")

    # # To save to a Markdown file
    with open(markdown_file, "w") as f:
        f.write("## HTTP Requests Metrics\n")
        f.write(results_table_markdown)
        f.write("\n\n")
        f.write("## Top 10 3rd Party Tracking Domains in First 6 Sessions\n")
        f.write(top_10_domains.to_markdown())
        f.write("\n\n")
        f.write("## Control Visits Only HTTP Requests Metrics\n")
        f.write(control_results_table_markdown)
        f.write("\n\n")
        f.write(
            "## Top 10 3rd Party Tracking Domains in First 6 Sessions (Control Visits Only)\n"
        )
        f.write(top_10_domains_control.to_markdown())

    print(f"FINISHED FOR {experiment_name}!")


else:
    print_timestamped_message(
        "The database rows and CSV file rows do not match. Please verify the data."
    )

# Close the database connection
conn.close()
print_timestamped_message("Script completed.")
