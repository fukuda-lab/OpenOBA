# The file was run from the repository root directory

from oba_crawler import OBAMeasurementExperiment

test_fashion_experiment = OBAMeasurementExperiment(
    "test_style_and_fashion_experiment_accept",
    False,
)

test_fashion_experiment.start(minutes=20)
