# The file was run from the fobam directory

from oba.oba_analysis import OBAAnalysis

# OBAAnalysis demo
# oba_quantifier_accept = OBAAnalysis(
#     experiment_name="style_and_fashion_experiment_accept",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_accept = oba_quantifier_accept.fetch_all_ads_grouped_by_browser_id()


# OBAAnalysis demo
oba_quantifier_reject = OBAAnalysis(
    experiment_name="style_and_fashion_experiment_reject",
    experiment_category="Style & Fashion",
)
ads_by_session_reject = oba_quantifier_reject.get_table_all_ads_grouped_by_browser_id()


# OBAAnalysis demo
# oba_quantifier_do_nothing = OBAAnalysis(
#     experiment_name="style_and_fashion_experiment_do_nothing",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_do_nothing = (
#     oba_quantifier_do_nothing.fetch_all_ads_grouped_by_browser_id()
# )

# print(ads_by_session_accept)
# print(ads_by_session_reject)
# print(ads_by_session_do_nothing)
