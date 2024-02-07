# The file was run from the fobam directory

from oba_crawler import OBAMeasurementExperiment

# We use the OBAMeasurementExperiment class to access the training pages handler
experiment = OBAMeasurementExperiment("training_pages_handler", True)

# We get the most popular pages by category that were confidently classified (we can access the ones that have cookie banner presence)
popular_sites_dict = (
    experiment.training_pages_handler.get_training_pages_grouped_by_category(
        k=100, confident=True, cookie_banner_found=None
    )
)

fashion_sites_with_cookie_banner = popular_sites_dict["Clothing"]["pages_urls"]

# fashion_experiment = OBAMeasurementExperiment(
#     "fashion_experiment_reject",
#     True,
#     cookie_banner_action=2,
#     do_clean_runs=False,
#     use_custom_pages=True,
#     custom_pages_params={
#         "categorize_pages": False,
#         "custom_pages_list": fashion_sites_with_cookie_banner,
#     },
# )

# fashion_experiment.start(minutes=5, browser_mode="native")

# fashion_experiment.crawl_to_reject_cookies_manually()