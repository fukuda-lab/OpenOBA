import sqlite3

experiment_name = "style_and_fashion_experiment_accept"
experiment_data_dir = f"/Volumes/FOBAM_data/8_days/datadir/{experiment_name}/"
sqlite_file = f"{experiment_data_dir}/crawl-data-copy.sqlite"

# javascript_script
javascript_script_url_file = f"{experiment_data_dir}/results/javascript_script_url.txt"
with sqlite3.connect(sqlite_file) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT script_url FROM javascript")
    results = cursor.fetchall()
    with open(javascript_script_url_file, "w") as file:
        file.writelines(f"'{row[0]}',\n" for row in results)

# http_request
http_requests_url_file = f"{experiment_data_dir}/results/http_requests_url.txt"
with sqlite3.connect(sqlite_file) as conn2:
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT url FROM http_requests")
    results2 = cursor2.fetchall()
    with open(http_requests_url_file, "w") as file2:
        file2.writelines(f"'{row[0]}',\n" for row in results2)
