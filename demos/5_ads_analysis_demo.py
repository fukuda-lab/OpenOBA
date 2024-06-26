from oba.experiment_metrics import ExperimentMetrics
import os
import pandas as pd

# PLOTS_DIR = "/Volumes/LaCie/OpenOBA/PLOTS/"
# RESULTS_DIR = "/Volumes/LaCie/OpenOBA/RESULTS/"
RESULTS_DIR = (
    f"/{os.getcwd()}/datadir/test_style_and_fashion_experiment_accept/results/"
)
PLOTS_DIR = RESULTS_DIR
NUMBER_OF_BROWSERS_RUN = 1


# # ----- DATA FOR THE EXPERIMENT -----

metrics_accept = ExperimentMetrics(
    "test_style_and_fashion_experiment_accept", control_runs=False
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
for i in range(NUMBER_OF_BROWSERS_RUN):
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
