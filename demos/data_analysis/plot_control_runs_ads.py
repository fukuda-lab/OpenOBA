# The file was run from the fobam directory

import pandas as pd
from oba.oba_analysis import OBAQuantifier
from oba.experiment_metrics import ExperimentMetrics

RESULTS_DIR = "/Volumes/FOBAM_data/RESULTS/"


def get_metrics_plot_and_tables(
    experiment_name: str, cookie_banner_option: int, category: str
):
    oba_quantifier = OBAQuantifier(
        experiment_name=experiment_name,
        experiment_category=category,
    )
    ads_by_session = oba_quantifier.fetch_all_ads_by_browser_id_as_dict()
    oba_quantifier.plot_ads_by_browser_id(
        ads_by_session, cookie_banner_option=cookie_banner_option
    )
    oba_quantifier.disconnect()

    experiment_metrics = ExperimentMetrics(experiment_name)

    experiment_summary_df = experiment_metrics.get_experiment_summary()

    numvisits_by_browser_id_and_url = (
        experiment_metrics.get_control_visits_by_url_and_browser()
    )

    experiment_control_name = (
        f"control_run_{['do_nothing', 'accept', 'reject'][cookie_banner_option]}"
    )
    control_runs = OBAQuantifier(
        experiment_name=experiment_control_name,
        experiment_category=category,
        control_runs=True,
    )

    control_run_ads = (
        control_runs.fetch_all_ads_grouped_by_artificial_sessions_and_site_url(
            numvisits_by_browser_id_and_url
        )
    )

    control_runs.plot_ads_by_browser_id(
        control_run_ads, cookie_banner_option=cookie_banner_option
    )

    control_runs.disconnect()

    experiment_control_metrics = ExperimentMetrics(
        experiment_control_name, control_runs=True
    )
    control_summary_df = experiment_control_metrics.get_experiment_summary()

    # Now extend experiment_summary_df with the control_summary_df adding the column "instance" with the value "experiment_name" and "experiment_name_control" respectively
    experiment_summary_df["instance"] = experiment_name
    control_summary_df["instance"] = experiment_control_name

    # Put instance column in the first position
    cols = list(experiment_summary_df.columns)
    cols = [cols[-1]] + cols[:-1]
    experiment_summary_df = experiment_summary_df[cols]
    control_summary_df = control_summary_df[cols]

    # Concatenate the two dataframes
    summary_df = pd.concat([experiment_summary_df, control_summary_df])

    return summary_df


# Call the new function with the appropriate arguments
summary_df_accept = get_metrics_plot_and_tables(
    experiment_name="style_and_fashion_experiment_accept",
    cookie_banner_option=1,
    category="Style & Fashion",
)

# Call the new function with the appropriate arguments
summary_df_reject = get_metrics_plot_and_tables(
    experiment_name="style_and_fashion_experiment_reject",
    cookie_banner_option=2,
    category="Style & Fashion",
)

# Call the new function with the appropriate arguments
summary_df_do_nothing = get_metrics_plot_and_tables(
    experiment_name="style_and_fashion_experiment_do_nothing",
    cookie_banner_option=0,
    category="Style & Fashion",
)

# Concatenate the three dataframes
summary_df = pd.concat([summary_df_accept, summary_df_reject, summary_df_do_nothing])

print(summary_df)

# Write the summary_df to a markdown file
summary_df.to_markdown(RESULTS_DIR + "summary_df.md")

# # Experiment Accept
# # OBAQuantifier
# oba_quantifier_accept = OBAQuantifier(
#     experiment_name="style_and_fashion_experiment_accept",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_accept = oba_quantifier_accept.fetch_all_ads_by_browser_id_as_dict()
# oba_quantifier_accept.plot_ads_by_browser_id(
#     ads_by_session_accept, cookie_banner_option=1
# )

# oba_quantifier_accept.disconnect()

# # Experiment Metrics to get the number of visits by URL
# experiment_metrics_accept = ExperimentMetrics("style_and_fashion_experiment_accept")

# numvisits_by_browser_id_and_url = (
#     experiment_metrics_accept.get_control_visits_by_url_and_browser()
# )

# experiment_summary_accept = experiment_metrics_accept.get_experiment_summary()

# control_runs_accept = OBAQuantifier(
#     experiment_name="control_run_accept",
#     experiment_category="Style & Fashion",
#     control_runs=True,
# )

# control_run_accept_ads = (
#     control_runs_accept.fetch_all_ads_grouped_by_artificial_sessions_and_site_url(
#         numvisits_by_browser_id_and_url
#     )
# )

# control_runs_accept.plot_ads_by_browser_id(
#     control_run_accept_ads, cookie_banner_option=1
# )

# control_runs_accept.disconnect()


# # Experiment reject
# # OBAQuantifier
# oba_quantifier_reject = OBAQuantifier(
#     experiment_name="style_and_fashion_experiment_reject",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_reject = oba_quantifier_reject.fetch_all_ads_by_browser_id_as_dict()
# oba_quantifier_reject.plot_ads_by_browser_id(
#     ads_by_session_reject, cookie_banner_option=2
# )
# oba_quantifier_reject.disconnect()

# # Experiment Metrics to get the number of visits by URL
# experiment_metrics_reject = ExperimentMetrics("style_and_fashion_experiment_reject")

# numvisits_by_browser_id_and_url = (
#     experiment_metrics_reject.get_control_visits_by_url_and_browser()
# )

# control_runs_reject = OBAQuantifier(
#     experiment_name="control_run_reject",
#     experiment_category="Style & Fashion",
#     control_runs=True,
# )

# control_run_reject_ads = (
#     control_runs_reject.fetch_all_ads_grouped_by_artificial_sessions_and_site_url(
#         numvisits_by_browser_id_and_url
#     )
# )

# control_runs_reject.plot_ads_by_browser_id(
#     control_run_reject_ads, cookie_banner_option=2
# )

# control_runs_reject.disconnect()


# # Experiment do_nothing
# # OBAQuantifier
# oba_quantifier_do_nothing = OBAQuantifier(
#     experiment_name="style_and_fashion_experiment_do_nothing",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_do_nothing = (
#     oba_quantifier_do_nothing.fetch_all_ads_by_browser_id_as_dict()
# )
# oba_quantifier_do_nothing.plot_ads_by_browser_id(
#     ads_by_session_do_nothing, cookie_banner_option=0
# )
# oba_quantifier_do_nothing.disconnect()

# # Experiment Metrics to get the number of visits by URL
# experiment_metrics_do_nothing = ExperimentMetrics(
#     "style_and_fashion_experiment_do_nothing"
# )

# numvisits_by_browser_id_and_url = (
#     experiment_metrics_do_nothing.get_control_visits_by_url_and_browser()
# )

# control_runs_do_nothing = OBAQuantifier(
#     experiment_name="control_run_do_nothing",
#     experiment_category="Style & Fashion",
#     control_runs=True,
# )

# control_run_do_nothing_ads = (
#     control_runs_do_nothing.fetch_all_ads_grouped_by_artificial_sessions_and_site_url(
#         numvisits_by_browser_id_and_url
#     )
# )

# control_runs_do_nothing.plot_ads_by_browser_id(
#     control_run_do_nothing_ads, cookie_banner_option=0
# )

# control_runs_do_nothing.disconnect()
