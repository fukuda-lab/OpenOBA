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
    "test_nohup_style_and_fashion_experiment_accept",
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

test_fashion_experiment.start(minutes=30)
