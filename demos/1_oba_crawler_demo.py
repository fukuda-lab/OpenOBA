# The file was run from the repository root directory

from oba_crawler import OBAMeasurementExperiment

test_fashion_experiment = OBAMeasurementExperiment(
    experiment_name="test_style_and_fashion_experiment_accept",
    fresh_experiment=True,
    do_clean_runs=False,
    cookie_banner_action=1,
    # browser_display_mode="native",
)

# We get the most popular pages by category that were confidently classified and with cookie banner presence (we can access the ones that have cookie banner presence)
test_fashion_experiment.set_training_pages_by_category(
    category="Style & Fashion",
    size=50,
    confident=1,
    cookie_banner_checked=1,
    cookie_banner_presence=1,
)

test_fashion_experiment.start(minutes=20)
