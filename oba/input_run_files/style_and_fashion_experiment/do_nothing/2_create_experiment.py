# The file was run from the fobam directory

from oba_crawler import OBAMeasurementExperiment


# fashion_experiment = OBAMeasurementExperiment(
#     "style_and_fashion_experiment_do_nothing",
#     True,
#     do_clean_runs=False,
#     cookie_banner_action=0,
# )

# # We get the most popular pages by category that were confidently classified and with cookie banner presence (we can access the ones that have cookie banner presence)
# fashion_experiment.set_training_pages_by_category(
#     category="Style & Fashion",
#     size=50,
#     confident=1,
#     cookie_banner_checked=1,
#     cookie_banner_presence=1,
# )

# fashion_experiment.start(minutes=5)


fashion_experiment = OBAMeasurementExperiment(
    "style_and_fashion_experiment_do_nothing",
    True,
    do_clean_runs=True,
    cookie_banner_action=0,
)

# We get the most popular pages by category that were confidently classified and with cookie banner presence (we can access the ones that have cookie banner presence)
fashion_experiment.set_training_pages_by_category(
    category="Style & Fashion",
    size=50,
    confident=1,
    cookie_banner_checked=1,
    cookie_banner_presence=1,
)

fashion_experiment.start(8)
