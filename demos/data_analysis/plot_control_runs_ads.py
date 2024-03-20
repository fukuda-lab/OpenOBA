# The file was run from the fobam directory

from oba.oba_analysis import OBAQuantifier

# OBAQuantifier demo
oba_quantifier_accept = OBAQuantifier(
    experiment_name="control_run_accept",
    experiment_category="Style & Fashion",
)
ads_by_session_accept = oba_quantifier_accept.fetch_all_ads_grouped_by_browser_id()
print(ads_by_session_accept)
# control_runs_list_accept = [
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://weatherbase.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://myforecast.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://theweathernetwork.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weather2umbrella.com/",
#     "http://weatherbase.com/",
# ]

# len(control_runs_list_accept)

# OBAQuantifier demo
# oba_quantifier_reject = OBAQuantifier(
#     experiment_name="control_run_reject",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_reject = oba_quantifier_reject.fetch_all_ads_grouped_by_browser_id()


# OBAQuantifier demo
# oba_quantifier_do_nothing = OBAQuantifier(
#     experiment_name="control_run_do_nothing",
#     experiment_category="Style & Fashion",
# )
# ads_by_session_do_nothing = (
#     oba_quantifier_do_nothing.fetch_all_ads_grouped_by_browser_id()
# )

# print(ads_by_session_accept)
# print(ads_by_session_reject)
# print(ads_by_session_do_nothing)

# oba_quantifier_accept.plot_ads_by_browser_id(
#     ads_by_session_accept, cookie_banner_option=1
# )
# oba_quantifier_reject.plot_ads_by_browser_id(
#     ads_by_session_reject, cookie_banner_option=2
# )

# oba_quantifier_do_nothing.plot_ads_by_browser_id(
#     ads_by_session_do_nothing, cookie_banner_option=0
# )
