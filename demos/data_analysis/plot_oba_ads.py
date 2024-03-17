# The file was run from the fobam directory

from oba.oba_analysis import OBAQuantifier

# OBAQuantifier demo
oba_quantifier_accept = OBAQuantifier(
    experiment_name="style_and_fashion_experiment_accept",
    experiment_category="Style & Fashion",
)
ads_by_session_accept = oba_quantifier_accept.fetch_all_ads_grouped_by_browser_id()


# OBAQuantifier demo
oba_quantifier_reject = OBAQuantifier(
    experiment_name="style_and_fashion_experiment_reject",
    experiment_category="Style & Fashion",
)
ads_by_session_reject = oba_quantifier_reject.fetch_all_ads_grouped_by_browser_id()


# OBAQuantifier demo
oba_quantifier_do_nothing = OBAQuantifier(
    experiment_name="style_and_fashion_experiment_do_nothing",
    experiment_category="Style & Fashion",
)
ads_by_session_do_nothing = (
    oba_quantifier_do_nothing.fetch_all_ads_grouped_by_browser_id()
)

# print(ads_by_session_accept)
# print(ads_by_session_reject)
# print(ads_by_session_do_nothing)

oba_quantifier_accept.plot_ads_by_browser_id(
    ads_by_session_accept, cookie_banner_option=1
)
oba_quantifier_reject.plot_ads_by_browser_id(
    ads_by_session_reject, cookie_banner_option=2
)

oba_quantifier_do_nothing.plot_ads_by_browser_id(
    ads_by_session_do_nothing, cookie_banner_option=0
)
