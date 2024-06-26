import pandas as pd

DATA_DIR = "/Volumes/LaCie/OpenOBA/oba_runs/"
RESULTS_DIR = "/Volumes/LaCie/OpenOBA/RESULTS/"


# For HTTP Requests
def get_all_third_party_tracking_domains(experiment_name):
    csv_path = (
        f"{DATA_DIR}/{experiment_name}/results/unique_tracking_3rd_party_domains.csv"
    )

    # The header of the file is request_url_domain,505764208,575187525,602318429,1686948626,2186715125,2674878447,2902477846,3116123078,3282406616
    # The first column is the domain name, so we will return all the values from the first column
    with open(csv_path, "r") as f:
        return set([line.split(",")[0] for line in f.readlines()])


# For Cookies
def get_all_third_party_tracking_cookie_domains(experiment_name):
    csv_path = f"{DATA_DIR}/{experiment_name}/results/cookies_host_check.csv"

    # This one is more complicated because the header is:
    # id,host,name,value,browser_id,visit_url,control_visit,host_domain,easyprivacy,easylist,adserverlist,visit_url_domain,third_party
    # So we will load the file in a pandas dataframe and return the unique values of the host_domain column that have third_party = True and (easyprivacy = True or easylist = True or adserverlist = True)
    df = pd.read_csv(csv_path)
    df = df[
        (df["third_party"] == True)
        & (
            (df["easyprivacy"] == True)
            | (df["easylist"] == True)
            | (df["adserverlist"] == True)
        )
    ]
    return set(df["host_domain"].unique())


accept_domains = get_all_third_party_tracking_domains(
    "style_and_fashion_experiment_accept"
)

reject_domains = get_all_third_party_tracking_domains(
    "style_and_fashion_experiment_reject"
)

do_nothing_domains = get_all_third_party_tracking_domains(
    "style_and_fashion_experiment_do_nothing"
)

# ___FIRST FOR HTTP REQUESTS___
# 1. Domains in accept_domains but not in reject_domains or do_nothing_domains
unique_accept_domains = accept_domains - reject_domains - do_nothing_domains
# 2. Domains in reject_domains but not in accept_domains or do_nothing_domains
unique_reject_domains = reject_domains - accept_domains - do_nothing_domains
# 3. Domains in do_nothing_domains but not in accept_domains or reject_domains
unique_do_nothing_domains = do_nothing_domains - accept_domains - reject_domains
# 4. Domains that appear in all three sets
common_domains = accept_domains & reject_domains & do_nothing_domains

# ___THEN FOR COOKIES___
accept_cookie_domains = get_all_third_party_tracking_cookie_domains(
    "style_and_fashion_experiment_accept"
)

reject_cookie_domains = get_all_third_party_tracking_cookie_domains(
    "style_and_fashion_experiment_reject"
)

do_nothing_cookie_domains = get_all_third_party_tracking_cookie_domains(
    "style_and_fashion_experiment_do_nothing"
)

# 1. Domains in accept_cookie_domains but not in reject_cookie_domains or do_nothing_cookie_domains
unique_accept_cookie_domains = (
    accept_cookie_domains - reject_cookie_domains - do_nothing_cookie_domains
)
# 2. Domains in reject_cookie_domains but not in accept_cookie_domains or do_nothing_cookie_domains
unique_reject_cookie_domains = (
    reject_cookie_domains - accept_cookie_domains - do_nothing_cookie_domains
)
# 3. Domains in do_nothing_cookie_domains but not in accept_cookie_domains or reject_cookie_domains
unique_do_nothing_cookie_domains = (
    do_nothing_cookie_domains - accept_cookie_domains - reject_cookie_domains
)
# 4. Domains that appear in all three sets
common_cookie_domains = (
    accept_cookie_domains & reject_cookie_domains & do_nothing_cookie_domains
)

# Save the HTTP results to a file
with open(RESULTS_DIR + "http_domains_analysis_per_instance.txt", "w") as f:
    f.write(f"=====ACCEPT ONLY DOMAINS===== / len={len(unique_accept_domains)}\n")
    f.write("\n".join(unique_accept_domains))
    f.write("\n\n\n\n\n")

    f.write(f"=====REJECT ONLY DOMAINS===== / len={len(unique_reject_domains)}\n")
    f.write("\n".join(unique_reject_domains))
    f.write("\n\n\n\n\n")

    f.write(
        f"=====DO NOTHING ONLY DOMAINS===== / len={len(unique_do_nothing_domains)}\n"
    )
    f.write("\n".join(unique_do_nothing_domains))
    f.write("\n\n\n\n\n")

    f.write(f"=====COMMON DOMAINS===== / len={len(common_domains)}\n")
    f.write("\n".join(common_domains))
    f.write("\n\n\n\n\n")

# Save the Cookies results to a file
with open(RESULTS_DIR + "cookies_domains_analysis_per_instance.txt", "w") as f:
    f.write(
        f"=====ACCEPT ONLY DOMAINS=====  / len={len(unique_accept_cookie_domains)}\n"
    )
    f.write("\n".join(unique_accept_cookie_domains))
    f.write("\n\n\n\n\n")

    f.write(
        f"=====REJECT ONLY DOMAINS=====  / len={len(unique_reject_cookie_domains)}\n"
    )
    f.write("\n".join(unique_reject_cookie_domains))
    f.write("\n\n\n\n\n")

    f.write(
        f"=====DO NOTHING ONLY DOMAINS=====  / len={len(unique_do_nothing_cookie_domains)}\n"
    )
    f.write("\n".join(unique_do_nothing_cookie_domains))
    f.write("\n\n\n\n\n")

    f.write(f"=====COMMON DOMAINS=====  / len={len(common_cookie_domains)}\n")
    f.write("\n".join(common_cookie_domains))
    f.write("\n\n\n\n\n")
