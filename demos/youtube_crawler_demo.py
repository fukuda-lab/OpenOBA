# The file was run from the repository root directory

from oba_crawler import OBAMeasurementExperiment


test_fashion_experiment = OBAMeasurementExperiment(
    experiment_name="youtube_demo_experiment",
    fresh_experiment=True,
    do_clean_runs=False,
    cookie_banner_action=0,
    control_visits_urls=[
        "https://www.youtube.com/watch?v=IamoTEgX94I",
        "https://www.youtube.com/watch?v=5twveLmWhvI",
        "https://www.youtube.com/watch?v=zLwhTIeSi68",
    ],
    browser_display_mode="native",
)

test_fashion_experiment.start(minutes=20, random_run=True, youtube=True)
