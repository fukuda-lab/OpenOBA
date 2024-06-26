import pandas as pd

DATA_DIR = "/Volumes/LaCie/OpenOBA/oba_runs/"
RESULTS_DIR = "/Volumes/LaCie/OpenOBA/RESULTS/"


# For HTTP Requests
def get_all_ad_domains(experiment_name):
    csv_path = f"{DATA_DIR}/{experiment_name}/results/ad_domains_all.csv"

    # The header of the file is adurl_domain,group
    # The first column is the domain name and the second column is the group, so we will read the file and return a list of tuples with the domain and the group
    with open(csv_path, "r") as f:
        domains = set()
        for line in f.readlines():
            # skip the header
            if line.startswith("adurl_domain"):
                continue
            domain, group = line.split(",")
            domains.add((domain, group.strip()))

        return domains


accept_domains = get_all_ad_domains("style_and_fashion_experiment_accept")

reject_domains = get_all_ad_domains("style_and_fashion_experiment_reject")

do_nothing_domains = get_all_ad_domains("style_and_fashion_experiment_do_nothing")

# Union of all domains
all_domains = accept_domains | reject_domains | do_nothing_domains

# 1. Domains in accept_domains but not in reject_domains or do_nothing_domains
unique_accept_domains = accept_domains - reject_domains - do_nothing_domains
# 2. Domains in reject_domains but not in accept_domains or do_nothing_domains
unique_reject_domains = reject_domains - accept_domains - do_nothing_domains
# 3. Domains in do_nothing_domains but not in accept_domains or reject_domains
unique_do_nothing_domains = do_nothing_domains - accept_domains - reject_domains
# 4. Domains that appear in all three sets
common_domains = accept_domains & reject_domains & do_nothing_domains


# Save the HTTP results to a file
with open(RESULTS_DIR + "ads_domains_analysis_per_instance.txt", "w") as f:
    f.write(f"=====ACCEPT ONLY DOMAINS===== / len={len(unique_accept_domains)}\n")
    unique_accept_domains_str = [
        f"{domain},{group}" for domain, group in unique_accept_domains
    ]
    f.write("\n".join(unique_accept_domains_str))
    f.write("\n\n\n\n\n")

    f.write(f"=====REJECT ONLY DOMAINS===== / len={len(unique_reject_domains)}\n")
    unique_reject_domains_str = [
        f"{domain},{group}" for domain, group in unique_reject_domains
    ]
    f.write("\n".join(unique_reject_domains_str))
    f.write("\n\n\n\n\n")

    f.write(
        f"=====DO NOTHING ONLY DOMAINS===== / len={len(unique_do_nothing_domains)}\n"
    )
    unique_do_nothing_domains_str = [
        f"{domain},{group}" for domain, group in unique_reject_domains
    ]
    f.write("\n".join(unique_do_nothing_domains_str))
    f.write("\n\n\n\n\n")

    f.write(f"=====COMMON DOMAINS===== / len={len(common_domains)}\n")
    common_domains_str = [f"{domain},{group}" for domain, group in common_domains]
    f.write("\n".join(common_domains_str))
    f.write("\n\n\n\n\n")
