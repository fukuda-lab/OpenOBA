from oba.experiment_metrics import ExperimentMetrics

# ----- DATA FOR THE EXPERIMENT -----

metrics_accept = ExperimentMetrics(
    "style_and_fashion_experiment_accept", control_runs=False
)
ads_by_category_accept = metrics_accept.get_ads_by_category_table_all_browsers()
metrics_accept.write_results_file_with_data(
    ads_by_category_accept, "ads_by_category.csv"
)
metrics_accept.close()

# metrics_reject = ExperimentMetrics(
#     "style_and_fashion_experiment_reject", control_runs=False
# )
# ads_by_category_reject = metrics_reject.get_ads_by_category_table_all_browsers()
# metrics_reject.write_results_file_with_data(
#     ads_by_category_reject, "ads_by_category.csv"
# )
# metrics_reject.close()

# metrics_do_nothing = ExperimentMetrics(
#     "style_and_fashion_experiment_do_nothing", control_runs=False
# )
# ads_by_category_do_nothing = metrics_do_nothing.get_ads_by_category_table_all_browsers()
# metrics_do_nothing.write_results_file_with_data(
#     ads_by_category_do_nothing, "ads_by_category.csv"
# )
# metrics_do_nothing.close()


# # ----- DATA FOR CONTROL RUNS -----

metrics_accept_control = ExperimentMetrics("control_run_accept", control_runs=True)
experiment_metrics = ExperimentMetrics("style_and_fashion_experiment_accept")
numvisits_by_browser_id_and_url = (
    experiment_metrics.get_control_visits_by_url_and_browser()
)
ads_by_category_accept = metrics_accept_control.get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
    numvisits_by_browser_id_and_url
)
metrics_accept_control.write_results_file_with_data(
    ads_by_category_accept, "ads_by_category.csv"
)
metrics_accept_control.close()

# metrics_reject_control = ExperimentMetrics("control_run_reject", control_runs=True)
# experiment_metrics = ExperimentMetrics("style_and_fashion_experiment_reject")
# numvisits_by_browser_id_and_url = (
#     experiment_metrics.get_control_visits_by_url_and_browser()
# )
# ads_by_category_reject = metrics_reject_control.get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
#     numvisits_by_browser_id_and_url
# )
# metrics_reject_control.write_results_file_with_data(
#     ads_by_category_reject, "ads_by_category.csv"
# )
# metrics_reject_control.close()

# metrics_do_nothing_control = ExperimentMetrics(
#     "control_run_do_nothing", control_runs=True
# )
# experiment_metrics = ExperimentMetrics("style_and_fashion_experiment_do_nothing")
# numvisits_by_browser_id_and_url = (
#     experiment_metrics.get_control_visits_by_url_and_browser()
# )
# ads_by_category_do_nothing = metrics_do_nothing_control.get_ads_by_category_grouped_by_artificial_sessions_and_site_url(
#     numvisits_by_browser_id_and_url
# )
# metrics_do_nothing_control.write_results_file_with_data(
#     ads_by_category_do_nothing, "ads_by_category.csv"
# )
# metrics_do_nothing_control.close()
