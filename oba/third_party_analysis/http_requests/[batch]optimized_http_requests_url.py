import requests
from adblockparser import AdblockRules
import csv
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count, Manager


def fetch_rules(url):
    response = requests.get(url)
    return AdblockRules(response.text.splitlines())


def process_batch(data):
    urls, rules, rule_params, counter, lock, total_urls = data
    results = []
    for url in urls:
        result = [url]
        for rule in rules.values():
            result.append(rule.should_block(url, rule_params))
        results.append(result)
    # Update and log the progress
    with lock:
        counter.value += len(urls)
        now = datetime.now().strftime("%H:%M:%S")
        print(
            f"[{now}]Processed {counter.value} of {total_urls} URLs ({100 * counter.value / total_urls:.2f}% completed)"
        )
    return results


def main():
    # Configuration
    batch_size = 1000  # Optimal batch size to be determined based on system capability
    rule_params = {"third-party": True}
    num_processes = 4  # Number of CPU cores to use for multiprocessing

    # URLs for rules
    rule_urls = {
        "https://easylist.to/easylist/easyprivacy.txt",
        "https://easylist.to/easylist/easylist.txt",
        "https://pgl.yoyo.org/adservers/serverlist.php?hostformat=hosts&showintro=1&mimetype=plaintext",
    }

    # Fetch rules
    start_time = time.time()
    print("Fetching rules...")
    with Pool(num_processes) as pool:
        rules = dict(zip(rule_urls, pool.map(fetch_rules, rule_urls)))
    print(f"All rules fetched in {time.time() - start_time:.2f} seconds.")

    # File paths
    EXPERIMENT_NAME = "control_run_accept"
    EXPERIMENT_DIR = f"/Users/mateoormeno/Desktop/control_runs/{EXPERIMENT_NAME}/"
    http_requests_file = f"{EXPERIMENT_DIR}/results/http_requests_url.txt"
    csv_file = f"{EXPERIMENT_DIR}/results/http_requests_url.csv"

    # Read URLs and process in batches
    with open(http_requests_file, "r") as infile:
        urls = infile.read().splitlines()
    total_urls = len(urls)
    batches = [urls[i : i + batch_size] for i in range(0, len(urls), batch_size)]

    # Set up multiprocessing tools for progress tracking
    manager = Manager()
    counter = manager.Value("i", 0)
    lock = manager.Lock()

    data_for_pool = [
        (batch, rules, rule_params, counter, lock, total_urls) for batch in batches
    ]
    now = datetime.now().strftime("%H:%M:%S")

    print(f"[{now}] Starting URL processing...")
    start_processing_time = time.time()
    with Pool(num_processes) as pool:
        all_results = pool.map(process_batch, data_for_pool)

    # Write results to CSV
    with open(csv_file, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["url", "easyprivacy", "easylist", "adserverlist"])
        for results in all_results:
            writer.writerows(results)

    print(f"All URLs processed in {time.time() - start_processing_time:.2f} seconds.")
    print(f"Total runtime: {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
