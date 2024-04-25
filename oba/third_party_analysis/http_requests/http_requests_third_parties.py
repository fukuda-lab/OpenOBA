import pandas as pd
import sqlite3
from datetime import datetime
from tqdm import tqdm
import tldextract


def print_timestamped_message(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}")


print_timestamped_message("Script started...")

DATA_DIR = "/Volumes/LaCie/OpenOBA/control_runs"
experiment_name = "control_run_accept"
db_path = f"{DATA_DIR}/{experiment_name}/crawl-data.sqlite"
csv_path = f"{DATA_DIR}/{experiment_name}/results/http_requests_url_2.csv"
# db_path = f"{DATA_DIR}/{experiment_name}/crawl-data.sqlite"
# csv_path = f"{DATA_DIR}/{experiment_name}/results/http_requests_url.csv"

# DATA_DIR = "/Volumes/LaCie/OpenOBA/oba_runs"
# experiment_name = "style_and_fashion_experiment_accept"
# db_path = f"{DATA_DIR}/{experiment_name}/crawl-data-copy.sqlite"
# csv_path = (
#     f"{DATA_DIR}/{experiment_name}/results/http_requests_url_with_control_visit.csv"
# )


new_csv_path = (
    f"{DATA_DIR}/{experiment_name}/results/http_requests_url_third_parties.csv"
)

# Connect to the SQLite database
print_timestamped_message("Connecting to database...")
conn = sqlite3.connect(db_path)

# Query to get the url from http_requests
print_timestamped_message("Querying SQLite database for control visit information...")
query = """
SELECT hr.id as id, 
       hr.url as url,
       sv.site_url as visit_url
FROM http_requests hr
JOIN site_visits sv ON hr.visit_id = sv.visit_id
ORDER BY hr.id
"""
df_http_requests_urls = pd.read_sql_query(query, conn)

print_timestamped_message(
    f"Query completed. Retrieved {len(df_http_requests_urls)} rows."
)

# Load the CSV file
print_timestamped_message("Loading CSV...")
csv_df = pd.read_csv(csv_path)

# Ensure the DataFrame from the database has the same number of rows as the CSV DataFrame
if len(df_http_requests_urls) == len(csv_df):
    print_timestamped_message("Merging DataFrames...")

    # Assuming the row order in the CSV and queried DataFrame matches exactly
    # Add new columns to the CSV df with the second level domain of the url and the visit_url, and the third party status according to the library
    request_url_domains = df_http_requests_urls["url"].apply(
        lambda x: tldextract.extract(x).registered_domain
    )

    visit_url_domains = df_http_requests_urls["visit_url"].apply(
        lambda x: tldextract.extract(x).registered_domain
    )

    # Add the third party status according to the library
    csv_df["third_party"] = request_url_domains != visit_url_domains

    csv_df["request_url_domain"] = request_url_domains
    csv_df["visit_url_domain"] = visit_url_domains

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
