# This file is meant to be run after having the data from the three instances of the experiment (accept, reject, do_nothing)
# Below is commented code for CONTROL/RANDOM RUNS analysis as well

from oba.experiment_metrics import ExperimentMetrics
import pandas as pd

PLOTS_DIR = "/Volumes/LaCie/OpenOBA/PLOTS/"
RESULTS_DIR = "/Volumes/LaCie/OpenOBA/RESULTS/"


# # ----- DATA FOR THE EXPERIMENT -----

metrics_accept = ExperimentMetrics(
    "style_and_fashion_experiment_accept", control_runs=False
)
accept_summary = metrics_accept.get_experiment_summary(is_control_run=False)
ads_oba_accept_df = metrics_accept.get_ads_by_category_table_all_browsers()
oba_accept_ads_all_domains = metrics_accept.count_ads_by_domain(ads_oba_accept_df)
oba_accept_ads_style_domains = metrics_accept.count_ads_by_domain(
    ads_oba_accept_df, category_filter="Style & Fashion"
)
oba_accept_ads_shopping_domains = metrics_accept.count_ads_by_domain(
    ads_oba_accept_df, category_filter="Shopping"
)

oba_accept_style_ads_1, oba_accept_style_ads_2 = metrics_accept.count_ads_by_session(
    ads_oba_accept_df, category_filter="Style & Fashion"
)
oba_accept_shopping_ads_1, oba_accept_shopping_ads_2 = (
    metrics_accept.count_ads_by_session(ads_oba_accept_df, category_filter="Shopping")
)

oba_accept_style_and_shopping_1 = []
oba_accept_style_and_shopping_2 = []
for i in range(6):
    oba_accept_style_and_shopping_1.append(
        oba_accept_style_ads_1[i] + oba_accept_shopping_ads_1[i]
    )
    oba_accept_style_and_shopping_2.append(
        oba_accept_style_ads_2[i] + oba_accept_shopping_ads_2[i]
    )


categories_oba_accept_df = metrics_accept.get_categories_total_and_unique_ads(
    ads_oba_accept_df
)
metrics_accept.create_plot_for_instance_categories_data(
    categories_oba_accept_df, "oba_accept_ads_categories", PLOTS_DIR
)
accept_evolution_df = metrics_accept.get_ads_evolution_by_session(
    ads_oba_accept_df, metrics_accept.oba_browsers
)
accept_evolution_shopping_df = metrics_accept.get_ads_evolution_by_session(
    ads_oba_accept_df, metrics_accept.oba_browsers, category_filter="Shopping"
)

metrics_reject = ExperimentMetrics(
    "style_and_fashion_experiment_reject", control_runs=False
)
reject_summary = metrics_reject.get_experiment_summary(is_control_run=False)
ads_oba_reject_df = metrics_reject.get_ads_by_category_table_all_browsers()
oba_reject_ads_all_domains = metrics_reject.count_ads_by_domain(ads_oba_reject_df)
oba_reject_ads_style_domains = metrics_reject.count_ads_by_domain(
    ads_oba_reject_df, category_filter="Style & Fashion"
)
oba_reject_ads_shopping_domains = metrics_reject.count_ads_by_domain(
    ads_oba_reject_df, category_filter="Shopping"
)
oba_reject_style_ads_1, oba_reject_style_ads_2 = metrics_reject.count_ads_by_session(
    ads_oba_reject_df, category_filter="Style & Fashion"
)
oba_reject_shopping_ads_1, oba_reject_shopping_ads_2 = (
    metrics_reject.count_ads_by_session(ads_oba_reject_df, category_filter="Shopping")
)
oba_reject_style_and_shopping_1 = []
oba_reject_style_and_shopping_2 = []
for i in range(6):
    oba_reject_style_and_shopping_1.append(
        oba_reject_style_ads_1[i] + oba_reject_shopping_ads_1[i]
    )
    oba_reject_style_and_shopping_2.append(
        oba_reject_style_ads_2[i] + oba_reject_shopping_ads_2[i]
    )
categories_oba_reject_df = metrics_reject.get_categories_total_and_unique_ads(
    ads_oba_reject_df
)
metrics_reject.create_plot_for_instance_categories_data(
    categories_oba_reject_df, "oba_reject_ads_categories", PLOTS_DIR
)
reject_evolution_df = metrics_reject.get_ads_evolution_by_session(
    ads_oba_reject_df, metrics_reject.oba_browsers
)
reject_evolution_shopping_df = metrics_reject.get_ads_evolution_by_session(
    ads_oba_reject_df, metrics_reject.oba_browsers, category_filter="Shopping"
)

metrics_do_nothing = ExperimentMetrics(
    "style_and_fashion_experiment_do_nothing", control_runs=False
)
do_nothing_summary = metrics_do_nothing.get_experiment_summary(is_control_run=False)
ads_oba_do_nothing_df = metrics_do_nothing.get_ads_by_category_table_all_browsers()
oba_do_nothing_ads_all_domains = metrics_do_nothing.count_ads_by_domain(
    ads_oba_do_nothing_df
)
oba_do_nothing_ads_style_domains = metrics_do_nothing.count_ads_by_domain(
    ads_oba_do_nothing_df, category_filter="Style & Fashion"
)
oba_do_nothing_ads_shopping_domains = metrics_do_nothing.count_ads_by_domain(
    ads_oba_do_nothing_df, category_filter="Shopping"
)
oba_do_nothing_style_ads_1, oba_do_nothing_style_ads_2 = (
    metrics_reject.count_ads_by_session(
        ads_oba_reject_df, category_filter="Style & Fashion"
    )
)
oba_do_nothing_shopping_ads_1, oba_do_nothing_shopping_ads_2 = (
    metrics_reject.count_ads_by_session(ads_oba_reject_df, category_filter="Shopping")
)
oba_do_nothing_style_and_shopping_1 = []
oba_do_nothing_style_and_shopping_2 = []
for i in range(6):
    oba_do_nothing_style_and_shopping_1.append(
        oba_do_nothing_style_ads_1[i] + oba_do_nothing_shopping_ads_1[i]
    )
    oba_do_nothing_style_and_shopping_2.append(
        oba_do_nothing_style_ads_2[i] + oba_do_nothing_shopping_ads_2[i]
    )

categories_oba_do_nothing_df = metrics_do_nothing.get_categories_total_and_unique_ads(
    ads_oba_do_nothing_df
)
metrics_do_nothing.create_plot_for_instance_categories_data(
    categories_oba_do_nothing_df, "oba_do_nothing_ads_categories", PLOTS_DIR
)
do_nothing_evolution_df = metrics_do_nothing.get_ads_evolution_by_session(
    ads_oba_do_nothing_df, metrics_do_nothing.oba_browsers
)
do_nothing_evolution_shopping_df = metrics_do_nothing.get_ads_evolution_by_session(
    ads_oba_do_nothing_df, metrics_do_nothing.oba_browsers, category_filter="Shopping"
)


# ExperimentMetrics.plot_ad_evolution(
#     accept_evolution_df,
#     do_nothing_evolution_df,
#     reject_evolution_df,
#     "ads_evolution_total",
#     PLOTS_DIR,
# )
# ExperimentMetrics.plot_ads_boxplot(
#     [
#         oba_accept_style_ads,
#         oba_do_nothing_style_ads,
#         oba_reject_style_ads,
#     ],
#     "ads_boxplot_style",
#     PLOTS_DIR,
# )
# ExperimentMetrics.plot_ads_boxplot(
#     [
#         oba_accept_shopping_ads,
#         oba_do_nothing_shopping_ads,
#         oba_reject_shopping_ads,
#     ],
#     "ads_boxplot_shopping",
#     PLOTS_DIR,
# )

# ExperimentMetrics.plot_ad_evolution(
#     accept_evolution_shopping_df,
#     do_nothing_evolution_shopping_df,
#     reject_evolution_shopping_df,
#     "ads_evolution_total_shopping",
#     PLOTS_DIR,
# )

ExperimentMetrics.plot_ad_providers_vs_other(
    [
        oba_accept_style_and_shopping_1,
        oba_accept_style_and_shopping_2,
        oba_do_nothing_style_and_shopping_1,
        oba_do_nothing_style_and_shopping_2,
        oba_reject_style_and_shopping_1,
        oba_reject_style_and_shopping_2,
    ],
    file_name="ads_by_domain_style_and_shopping",
    data_dir=PLOTS_DIR,
)


# # # ----- DATA FOR CONTROL/RANDOM RUNS -----

metrics_accept_control = ExperimentMetrics("control_run_accept", control_runs=True)
experiment_metrics = ExperimentMetrics("style_and_fashion_experiment_accept")
numvisits_by_browser_id_and_url = (
    experiment_metrics.get_control_visits_by_url_and_browser()
)
accept_control_summary = metrics_accept_control.get_experiment_summary(
    is_control_run=True, numvisits_by_browser_id_and_url=numvisits_by_browser_id_and_url
)
ads_control_accept_df = metrics_accept_control.get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
    numvisits_by_browser_id_and_url
)
control_accept_ads_all_domains = metrics_accept_control.count_ads_by_domain(
    ads_control_accept_df
)
control_accept_ads_style_domains = metrics_accept_control.count_ads_by_domain(
    ads_control_accept_df, category_filter="Style & Fashion"
)
control_accept_ads_shopping_domains = metrics_accept_control.count_ads_by_domain(
    ads_control_accept_df, category_filter="Shopping"
)

oba_accept_control_grouped = metrics_accept_control.count_ads_by_session_by_group(
    ads_control_accept_df
)
categories_control_accept_df = (
    metrics_accept_control.get_categories_total_and_unique_ads(ads_control_accept_df)
)
metrics_accept_control.create_plot_for_instance_categories_data(
    categories_control_accept_df, "control_accept_ads_categories", PLOTS_DIR
)

metrics_reject_control = ExperimentMetrics("control_run_reject", control_runs=True)
experiment_metrics = ExperimentMetrics("style_and_fashion_experiment_reject")
numvisits_by_browser_id_and_url = (
    experiment_metrics.get_control_visits_by_url_and_browser()
)
reject_control_summary = metrics_reject_control.get_experiment_summary(
    is_control_run=True, numvisits_by_browser_id_and_url=numvisits_by_browser_id_and_url
)
ads_control_reject_df = metrics_reject_control.get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
    numvisits_by_browser_id_and_url
)
control_reject_ads_all_domains = metrics_reject_control.count_ads_by_domain(
    ads_control_reject_df
)
control_reject_ads_style_domains = metrics_reject_control.count_ads_by_domain(
    ads_control_reject_df, category_filter="Style & Fashion"
)
control_reject_ads_shopping_domains = metrics_reject_control.count_ads_by_domain(
    ads_control_reject_df, category_filter="Shopping"
)
oba_reject_control_grouped = metrics_reject_control.count_ads_by_session_by_group(
    ads_control_reject_df
)
categories_control_reject_df = (
    metrics_reject_control.get_categories_total_and_unique_ads(ads_control_reject_df)
)
metrics_reject_control.create_plot_for_instance_categories_data(
    categories_control_reject_df, "control_reject_ads_categories", PLOTS_DIR
)

metrics_do_nothing_control = ExperimentMetrics(
    "control_run_do_nothing", control_runs=True
)
experiment_metrics = ExperimentMetrics("style_and_fashion_experiment_do_nothing")
numvisits_by_browser_id_and_url = (
    experiment_metrics.get_control_visits_by_url_and_browser()
)
do_nothing_control_summary = metrics_do_nothing_control.get_experiment_summary(
    is_control_run=True, numvisits_by_browser_id_and_url=numvisits_by_browser_id_and_url
)
ads_control_do_nothing_df = metrics_do_nothing_control.get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
    numvisits_by_browser_id_and_url
)
control_do_nothing_ads_all_domains = metrics_do_nothing_control.count_ads_by_domain(
    ads_control_do_nothing_df
)
control_do_nothing_ads_style_domains = metrics_do_nothing_control.count_ads_by_domain(
    ads_control_do_nothing_df, category_filter="Style & Fashion"
)
control_do_nothing_ads_shopping_domains = (
    metrics_do_nothing_control.count_ads_by_domain(
        ads_control_do_nothing_df, category_filter="Shopping"
    )
)
oba_do_nothing_control_grouped = (
    metrics_do_nothing_control.count_ads_by_session_by_group(ads_control_do_nothing_df)
)
categories_control_do_nothing_df = (
    metrics_do_nothing_control.get_categories_total_and_unique_ads(
        ads_control_do_nothing_df
    )
)
metrics_do_nothing_control.create_plot_for_instance_categories_data(
    categories_control_do_nothing_df, "control_do_nothing_ads_categories", PLOTS_DIR
)


# Make prints for the summaries
print("Accept")
print(accept_summary)
print("Control Accept")
print(accept_control_summary)

print("Reject")
print(reject_summary)
print("Control Reject")
print(reject_control_summary)

print("Do Nothing")
print(do_nothing_summary)
print("Control Do Nothing")
print(do_nothing_control_summary)


metrics_accept.close()
metrics_reject.close()
metrics_do_nothing.close()
metrics_accept_control.close()
metrics_reject_control.close()
metrics_do_nothing_control.close()


# oba_all_ads_by_domain = pd.concat(
#     [
#         oba_accept_ads_all_domains,
#         oba_reject_ads_all_domains,
#         oba_do_nothing_ads_all_domains,
#     ]
# )

# # Print the total sum of ads
# print("OBA Total sum of ads new criteria")
# print(oba_all_ads_by_domain["NumAds"].sum())

# oba_style_ads_by_domain = pd.concat(
#     [
#         oba_accept_ads_style_domains,
#         oba_reject_ads_style_domains,
#         oba_do_nothing_ads_style_domains,
#     ]
# )

# # Print the total sum of ads
# print("OBA Total sum of style ads new criteria")
# print(oba_style_ads_by_domain["NumAds"].sum())


# oba_shopping_ads_by_domain = pd.concat(
#     [
#         oba_accept_ads_shopping_domains,
#         oba_reject_ads_shopping_domains,
#         oba_do_nothing_ads_shopping_domains,
#     ]
# )

# # Print the total sum of ads
# print("OBA Total sum of shopping ads new criteria")
# print(oba_shopping_ads_by_domain["NumAds"].sum())


# control_all_ads_by_domain = pd.concat(
#     [
#         control_accept_ads_all_domains,
#         control_reject_ads_all_domains,
#         control_do_nothing_ads_all_domains,
#     ]
# )

# # Print the total sum of ads
# print("control Total sum of ads new criteria")
# print(control_all_ads_by_domain["NumAds"].sum())

# control_style_ads_by_domain = pd.concat(
#     [
#         control_accept_ads_style_domains,
#         control_reject_ads_style_domains,
#         control_do_nothing_ads_style_domains,
#     ]
# )

# # Print the total sum of ads
# print("control Total sum of style ads new criteria")
# print(control_style_ads_by_domain["NumAds"].sum())


# control_shopping_ads_by_domain = pd.concat(
#     [
#         control_accept_ads_shopping_domains,
#         control_reject_ads_shopping_domains,
#         control_do_nothing_ads_shopping_domains,
#     ]
# )

# # Print the total sum of ads
# print("control Total sum of shopping ads new criteria")
# print(control_shopping_ads_by_domain["NumAds"].sum())


# # We have one df with the domains (column "adurl_domain") and count of ads (column "NumAds") for each instance
# # We will merge the three DataFrames and then group by domain and sum the number of ads
# # We will then sort by the number of ads and save the result to a CSV file
# # Repeat for random (control) run
# control_ads_by_domain = pd.concat(
#     [
#         control_accept_ads_all_domains,
#         control_reject_ads_all_domains,
#         control_do_nothing_ads_all_domains,
#     ]
# )
# control_ads_by_domain = (
#     control_ads_by_domain.groupby("adurl_domain").sum().reset_index()
# )
# control_ads_by_domain = control_ads_by_domain.sort_values(by="NumAds", ascending=False)
# control_ads_by_domain.to_csv(
#     f"{RESULTS_DIR}/ads_filtered_domain_control_all_instances.csv", index=False
# )


# Given a list of domains, we want to know all the ad_urls that belong to those domains, for all instances, and the number of ads for each ad_url
# We will merge the three DataFrames, filter by the domains in the list, and count the number of ads for each ad_url, then save the result to a CSV file
# filter_domains = [
#     "thesearchgod.com",
#     "etoro.com",
#     "anlim.de",
#     "quelancepitylus.com",
#     "ilius.net",
#     "snzgdl.com",
#     "mysearchesnow.com",
#     "beleepstooked.com",
#     "linka.me",
#     "latellscoaddents.com",
#     "bibinboxputhwagon.com",
#     "onetag.com",
#     "tradetracker.net",
#     "zarbi.com",
#     "strateg.is",
#     "rtbhouse.com",
#     "integralads.com",
# ]

# oba_ads_df = pd.concat([ads_oba_accept_df, ads_oba_reject_df, ads_oba_do_nothing_df])
# ads_filtered = oba_ads_df[oba_ads_df["adurl_domain"].isin(filter_domains)]

# ads_filtered = (
#     ads_filtered.groupby(["adurl_domain", "ad_url"]).size().reset_index(name="NumAds")
# )

# ads_filtered.to_csv(f"{RESULTS_DIR}/list_filtered_ads_oba.csv", index=False)

# # Repeat for random (control) run
# control_ads_df = pd.concat(
#     [ads_control_accept_df, ads_control_reject_df, ads_control_do_nothing_df]
# )
# control_ads_filtered = control_ads_df[
#     control_ads_df["adurl_domain"].isin(filter_domains)
# ]

# control_ads_filtered = (
#     control_ads_filtered.groupby(["adurl_domain", "ad_url"])
#     .size()
#     .reset_index(name="NumAds")
# )

# control_ads_filtered.to_csv(f"{RESULTS_DIR}/list_filtered_ads_control.csv", index=False)
