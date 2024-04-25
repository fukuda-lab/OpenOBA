import pandas as pd
import sqlite3
from datetime import datetime
from tqdm import tqdm


def print_timestamped_message(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}")


print_timestamped_message("Script started...")

# Paths to your CSV file and SQLite database
csv_path = "/Volumes/FOBAM_data/8_days/datadir/style_and_fashion_experiment_do_nothing/results/javascript_script_url.csv"
new_csv_path = "/Volumes/FOBAM_data/8_days/datadir/style_and_fashion_experiment_do_nothing/results/javascript_script_url_with_control_visit.csv"
db_path = "/Volumes/FOBAM_data/8_days/datadir/style_and_fashion_experiment_do_nothing/crawl-data-copy.sqlite"

# Connect to the SQLite database
print_timestamped_message("Connecting to database...")
conn = sqlite3.connect(db_path)

# Query to join javascript with site_visits on visit_id and determine control_visit status based on site_rank
print_timestamped_message("Querying SQLite database for control visit information...")
query = """
SELECT js.id, 
       (sv.site_rank IS NOT NULL) AS control_visit
FROM javascript js
JOIN site_visits sv ON js.visit_id = sv.visit_id
ORDER BY js.id
"""
control_info_df = pd.read_sql_query(query, conn)

print_timestamped_message(
    f"Query completed. Retrieved {len(control_info_df)} rows of control visit information."
)

# Load the CSV file
print_timestamped_message("Loading CSV...")
csv_df = pd.read_csv(csv_path)

# Ensure the DataFrame from the database has the same number of rows as the CSV DataFrame
if len(control_info_df) == len(csv_df):
    print_timestamped_message("Merging DataFrames...")
    # Assuming the row order in the CSV and control_info_df matches exactly
    csv_df["control_visit"] = control_info_df["control_visit"].astype(bool)

    print_timestamped_message("Saving to new CSV...")
    csv_df.to_csv(new_csv_path, index=False)
    print_timestamped_message(f"New CSV saved to {new_csv_path}.")
else:
    print_timestamped_message(
        "The database rows and CSV file rows do not match. Please verify the data."
    )

# Close the database connection
conn.close()
print_timestamped_message("Script completed.")
