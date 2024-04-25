import json
import pandas as pd
import sqlite3
from datetime import datetime
from oba.experiment_metrics import ExperimentMetrics

# --- CASE CONTROL RUNS ---
DATA_DIR = "/Volumes/LaCie/OpenOBA/control_runs/"
experiment_name = "control_run_accept"
db_path = f"{DATA_DIR}/{experiment_name}/crawl-data.sqlite"
csv_path = (
    f"{DATA_DIR}/{experiment_name}/results/javascript_script_url_third_parties.csv"
)
markdown_file = (
    f"{DATA_DIR}/{experiment_name}/results/javascript_third_party_metrics.md"
)

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


def aggregate_data(group, summary=False):
    data = {
        "NumUniqueURL": group["url"].nunique(),
        "NumUniqueURL_3rdParty": group[group["third_party"]]["url"].nunique(),
        "NumUniqueURL_3rdParty_Tracking": group[
            (group["third_party"])
            & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
        ]["url"].nunique(),
        "NumUniqueDomain_3rdParty": group[group["third_party"]][
            "script_url_domain"
        ].nunique(),
        "NumUniqueDomain_3rdParty_Tracking": group[
            (group["third_party"])
            & (group["easyprivacy"] | group["easylist"] | group["adserverlist"])
        ]["script_url_domain"].nunique(),
    }

    if summary:
        data["browser_id"] = "-"

    return pd.Series(data)


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
    SELECT js.id AS id, fv.site_rank, fv.site_url, fv.visit_order, :browser_id AS browser_id
    FROM javascript js
    JOIN FilteredVisits fv ON js.visit_id = fv.visit_id
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


# Connect to the SQLite database
print_timestamped_message(f"Script started for {experiment_name}...")
print_timestamped_message("Connecting to database...")
conn = sqlite3.connect(db_path)

# Get the number of visits by browser_id and url in the OBA experiment
oba_experiment_metrics = ExperimentMetrics(oba_experiment_name)
numvisits_by_browser_id_and_url = (
    oba_experiment_metrics.get_control_visits_by_url_and_browser()
)

print(numvisits_by_browser_id_and_url)

# Initialize the sessions list and the set of seen browser_ids
site_visits_count = {site_url: 0 for _, site_url, _ in numvisits_by_browser_id_and_url}

session_number = 0  # Initialize the session number

cursor = conn.cursor()

js_df = pd.DataFrame(
    columns=["id", "site_rank", "site_url", "visit_order", "browser_id"]
)  # Initialize the DataFrame to store the JavaScript data

for browser_id, site_url, num_visits in numvisits_by_browser_id_and_url:
    # Fetch the ads from num_visits visits to the current site_url, starting from the first visit after the already taken visit
    query_result_df = fetch_entries_by_limits_and_site_url(
        cursor, site_url, num_visits, site_visits_count[site_url], browser_id
    )

    # Add the fetched data to the DataFrame
    js_df = pd.concat([js_df, query_result_df])

    # Update the number of visits taken from the site_url
    site_visits_count[site_url] += num_visits

cursor.close()

# Order df by id
js_df = js_df.sort_values("id").reset_index(drop=True)

print(js_df)

print_timestamped_message(f"Query completed. Retrieved {len(js_df)} rows.")

# Load the CSV file
print_timestamped_message("Loading CSV...")
csv_df = pd.read_csv(csv_path)

# Ensure the DataFrame from the database has the same number of rows as the CSV DataFrame
if len(js_df) == len(csv_df):
    print_timestamped_message("Merging DataFrames...")

    # Assuming the row order in the CSV and queried DataFrame matches exactly
    # Add new columns to the CSV df with the browser_id
    csv_df.insert(1, "browser_id", js_df["browser_id"])

    # Define a mask for third-party with any filter list true
    filter_mask = (
        csv_df["easyprivacy"] | csv_df["easylist"] | csv_df["adserverlist"]
    ) & csv_df["third_party"]

    # Perform groupby and aggregate operations
    aggregated_data = csv_df.groupby("browser_id").apply(aggregate_data)

    # Reset index if necessary to make 'browser_id' a column again
    aggregated_data.reset_index(inplace=True)

    aggregated_data["browser_id"] = pd.Categorical(
        aggregated_data["browser_id"], categories=oba_browser_ids_order, ordered=True
    )

    aggregated_data = aggregated_data.sort_values("browser_id")

    # Exclude row with browser_id = 0
    aggregated_data = aggregated_data[
        aggregated_data["browser_id"].isin(oba_browser_ids_order)
    ].reset_index(drop=True)

    # Obtain numbers for the combined first 6 sessions
    summary_data = aggregate_data(
        csv_df[csv_df["browser_id"].isin(oba_browser_ids_order[:6])], summary=True
    )
    # Convert to DataFrame for a single row result
    summary_row = pd.DataFrame([summary_data], index=["Combined First 6 Sessions"])

    # Append this summary row to the results_table
    results_table = pd.concat([aggregated_data, summary_row])
    results_table_markdown = results_table.to_markdown()

    # # To save to a Markdown file
    with open(markdown_file, "w") as f:
        f.write("## HTTP Requests Metrics\n")
        f.write(results_table_markdown)

    print(f"FINISHED FOR {experiment_name}!")

else:
    print_timestamped_message(
        "The database rows and CSV file rows do not match. Please verify the data."
    )

# Close the database connection
conn.close()
print_timestamped_message("Script completed.")
