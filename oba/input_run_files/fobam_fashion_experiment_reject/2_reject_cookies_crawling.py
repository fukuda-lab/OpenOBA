# The file was run from the fobam directory

from oba_crawler import OBAMeasurementExperiment

# We use the OBAMeasurementExperiment class to access the training pages handler
experiment = OBAMeasurementExperiment("training_pages_handler", True)

# We get the most popular pages by category that were confidently classified (we can access the ones that have cookie banner presence)
popular_sites_dict = (
    experiment.training_pages_handler.get_most_popular_pages_by_category(
        k=20, confident=True, cookie_banner_found=True
    )
)

fashion_sites_with_cookie_banner = popular_sites_dict["Fashion"]["page_urls"]
print(fashion_sites_with_cookie_banner)

fashion_experiment = OBAMeasurementExperiment(
    "fashion_experiment_reject",
    True,
    cookie_banner_action=2,
    use_custom_pages=True,
    custom_pages_params={
        "categorize_pages": False,
        "custom_pages_list": fashion_sites_with_cookie_banner,
    },
)

# fashion_experiment.crawl_to_reject_cookies_manually()
