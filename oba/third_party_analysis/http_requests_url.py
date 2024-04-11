import requests
from tqdm import tqdm
from adblockparser import AdblockRules
import csv

EXPERIMENT_NAME = "style_and_fashion_experiment_do_nothing"
EXPERIMENT_DIR = f"/Volumes/FOBAM_data/8_days/datadir/{EXPERIMENT_NAME}/"

easyprivacy_url = "https://easylist.to/easylist/easyprivacy.txt"
easyprivacy_content = requests.get(easyprivacy_url).text

easylist_url = "https://easylist.to/easylist/easylist.txt"
easylist_content = requests.get(easylist_url).text

adserverlist_url = "https://pgl.yoyo.org/adservers/serverlist.php?hostformat=hosts&showintro=1&mimetype=plaintext"
adserverlist_content = requests.get(adserverlist_url).text

easyprivacy_rules = AdblockRules(easyprivacy_content.splitlines())
easylist_rules = AdblockRules(easylist_content.splitlines())
adserverlist_rules = AdblockRules(adserverlist_content.splitlines())

rule_params = {"third-party": True}


http_requests_file = f"{EXPERIMENT_DIR}/results/http_requests_url.txt"
with open(http_requests_file, "r") as file:
    http_requests = file.read().splitlines()
csv_file = f"{EXPERIMENT_DIR}/results/http_requests_url.csv"
with open(csv_file, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["url", "easyprivacy", "easylist", "adserverlist"])
    for url in tqdm(http_requests):
        easyprivacy_result = easyprivacy_rules.should_block(url, rule_params)
        easylist_result = easylist_rules.should_block(url, rule_params)
        adserverlist_result = adserverlist_rules.should_block(url, rule_params)
        writer.writerow([url, easyprivacy_result, easylist_result, adserverlist_result])
