from oba_crawler import OBAMeasurementExperiment

fashion_experiment = OBAMeasurementExperiment(
    "style_and_fashion_experiment_reject",
    False,
)

fashion_experiment.start(8)
