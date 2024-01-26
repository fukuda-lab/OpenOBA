import glob
import json
import logging
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import AnyStr, DefaultDict, Dict, List, Optional, Set, Tuple

from enums import CleanBrowser, GenericQueries, OBABrowser
from extract_ad_url import process

# Create a logger
logger = logging.getLogger(__name__)
# EXPERIMENT_NAME = input('Experiment name to extract ads from:\n')
EXPERIMENT_NAME = "exp_advertising"


def _get_site_ads_from_visit(
    visit_id: AnyStr, source_pages_dir: AnyStr, static_ads: Set = None
) -> List:
    # Not sure about this way of doing it
    static_ads = set() if not static_ads else static_ads
    # Unpack row
    # Search for the JSON file using a pattern
    pattern = os.path.join(
        source_pages_dir, f"{visit_id}-*.json.gz"
    )  # Match all the ads
    json_files = glob.glob(pattern)

    href_urls = []
    landing_page_urls = []
    # Process each file that matches the pattern
    for json_filename in json_files:
        ad_url_data = process(["easylist.txt", "easyprivacy.txt"], json_filename)
        for _, value in ad_url_data.items():
            if value["landing_page_url"] not in static_ads:
                landing_page_urls.append(value["landing_page_url"])
                href_urls.append(value["href_url"])
    return (landing_page_urls, href_urls)


def get_static_ads(clean_visit_rows: List, source_pages_dir: AnyStr) -> Set:
    # We use a set because for each control_site, we don't want duplicated static ads since we will match them.
    static_ads = set()
    # try:
    # Traverse clean browser visits
    for clean_row in clean_visit_rows:
        _, visit_id = clean_row
        ads_tuple = _get_site_ads_from_visit(visit_id, source_pages_dir)
        static_ads = static_ads | set(ads_tuple[0])
    return static_ads

    # except Exception as e:
    # logger.error(f"ERROR OCURRED TRYING TO GET STATIC ADS: {str(e)}")


def get_control_sites_ads(
    control_sites_urls: List,
    oba_browser_queries: OBABrowser,
    output_dict,
    static_ads,
    source_pages_dir,
    cursor,
) -> DefaultDict[str, Dict]:
    """Return a dictionary of dictionaries of lists with control site urls as first keys, visit_ids as second keys each one with a list with the ads found in that visit

    We should looking on the categorization systems for ads, so I can get the categories of an ad somehow and this way know in a more automatized manner, if the ads belong to the shoes for woman topic."""
    # Start with the crawled data ads extraction
    for control_site_url in control_sites_urls:
        # We assume that each control site was visited only once during clean run.
        query = oba_browser_queries.get_visit_rows_per_control_site_query(
            control_site_url
        )
        # Fetch all the visits associated to the actual control site
        cursor.execute(query)
        control_site_visit_rows = cursor.fetchall()
        for visit_row in control_site_visit_rows:
            # Update the output dict with the ads gathered
            site_url, visit_id, site_rank = visit_row
            ads_found = _get_site_ads_from_visit(
                visit_id, source_pages_dir, static_ads=static_ads
            )
            if ads_found:
                # Add href_urls
                # output_dict['dynamic_ads'][site_url].append({site_rank: {'landing_page_urls': ads_found[0], 'href_urls': ads_found[1]}})
                # Add only landing_page_urls
                output_dict["dynamic_ads"][site_url].append({site_rank: ads_found[0]})


def read_browser_ids(experiment_name: str) -> Tuple:
    file_path = f"experiments/{experiment_name}_config.json"
    # File exists, load the existing JSON
    with open(file_path, "r") as f:
        browser_ids_dict = json.load(f)

    clear_browser_ids = browser_ids_dict["browser_ids"]["clear"]
    oba_browser_ids = browser_ids_dict["browser_ids"]["oba"]
    return clear_browser_ids, oba_browser_ids


# Connect to SQLite database
conn = sqlite3.connect(Path(f"../datadir/{EXPERIMENT_NAME}.sqlite"))
compressed_json_folder = f"../datadir/sources/{EXPERIMENT_NAME}"
cursor = conn.cursor()

# Fetch both browser ids (clean run and crawl)
clear_browser_ids, oba_browser_ids = read_browser_ids(experiment_name=EXPERIMENT_NAME)

output_data = {"dynamic_ads": defaultdict(list)}
static_ads = set()
control_site_urls = set()

for browser_id in clear_browser_ids:
    clear_browser_queries = CleanBrowser(browser_id=browser_id)
    # Get all the static and contextual ads (clean browser)
    cursor.execute(clear_browser_queries.CleanRunVisitsQuery)
    clean_run_site_visits = cursor.fetchall()
    static_ads = static_ads | get_static_ads(
        clean_run_site_visits, source_pages_dir=compressed_json_folder
    )

    # Get all the control_sites urls in a list
    if len(clean_run_site_visits) > len(control_site_urls):
        for control_site_url, _ in clean_run_site_visits:
            control_site_urls.add(control_site_url)
output_data["static_ads"] = list(static_ads)


# Get a dictionary with all the ads by control_site and visit_id ordered by site_rank
for browser_id in oba_browser_ids:
    oba_browser_queries = OBABrowser(browser_id=browser_id)
    # TODO: Check all the code of these scripts with ChatGPT
    get_control_sites_ads(
        control_site_urls,
        oba_browser_queries,
        output_data,
        static_ads,
        source_pages_dir=compressed_json_folder,
        cursor=cursor,
    )

# Write output_data to output JSON file
with open(f"results/{EXPERIMENT_NAME}/chronological_progress.json", "w") as output_file:
    json.dump(output_data, output_file, indent=2)

# Close the connection to the SQLite database
conn.close()
