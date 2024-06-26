from oba_crawler import OBAMeasurementExperiment

fashion_experiment = OBAMeasurementExperiment(
    "style_and_fashion_experiment_accept",
    False,
)

fashion_experiment.start(8)
