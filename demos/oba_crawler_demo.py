# The file was run from the fobam directory

from oba_crawler import OBAMeasurementExperiment

# We use the OBAMeasurementExperiment class to access the training pages handler
# experiment = OBAMeasurementExperiment("training_pages_handler", True)

# We get the most popular pages by category that were confidently classified (we can access the ones that have cookie banner presence)
# popular_sites_dict = (
#     experiment.training_pages_handler.get_training_pages_grouped_by_category(
#         k=100, confident=True, cookie_banner_found=None
#     )
# )

# fashion_sites_with_cookie_banner = popular_sites_dict["Clothing"]["pages_urls"]

test_fashion_experiment = OBAMeasurementExperiment(
    "test_style_and_fashion_experiment_accept",
    True,
    do_clean_runs=False,
    cookie_banner_action=1,
)

# We get the most popular pages by category that were confidently classified and with cookie banner presence (we can access the ones that have cookie banner presence)
test_fashion_experiment.set_training_pages_by_category(
    category="Style & Fashion",
    size=50,
    confident=1,
    cookie_banner_checked=1,
    cookie_banner_presence=1,
)

test_fashion_experiment.start(minutes=5)


# DataProcesser demo
# style_and_fashion_experiment_accept_data_processer = DataProcesser(
#     "test_style_and_fashion_experiment_accept", WEBSHRINKER_CREDENTIALS
# )

# style_and_fashion_experiment_accept_data_processer.filter_ads(
#     non_ads=True, unspecific_ads=True
# )
# style_and_fashion_experiment_accept_data_processer.update_crawling_data_process()


# OBAQuantifier demo
# oba_quantifier = OBAQuantifier(
#     experiment_name="test_style_and_fashion_experiment_accept",
#     category_to_analyze="Style & Fashion",
# )
# organized_data = oba_quantifier.fetch_control_site_ads_breakdown()
# oba_quantifier.generate_markdown_tables(organized_data)
# aggregated_data = oba_quantifier.fetch_aggregated_ads_breakdown()
# oba_quantifier.generate_aggregated_markdown_table(aggregated_data)
# oba_quantifier.visualize_ob_advertising_evolution_by_visit()
