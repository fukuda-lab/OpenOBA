from oba_crawler import OBAMeasurementExperiment

fashion_experiment = OBAMeasurementExperiment(
    "style_and_fashion_experiment_do_nothing",
    False,
)

fashion_experiment.start(8)
