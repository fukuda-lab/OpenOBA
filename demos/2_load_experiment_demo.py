# The file was run from the repository root directory

from oba_crawler import OBAMeasurementExperiment

test_fashion_experiment = OBAMeasurementExperiment(
    "test_style_and_fashion_experiment_accept",
    False,
    # browser_display_mode="native",
)

test_fashion_experiment.start(minutes=30, control_visits_rate=50)
