# The file was run from the fobam directory

from oba.data_processer import DataProcesser

WEBSHRINKER_CREDENTIALS = {
    "api_key": "GhU39K7bdfvdxRlcnEkT",
    "secret_key": "ZwnCzHIpw08DF10Fmz5c",
}

# DataProcesser demo
style_and_fashion_experiment_accept_data_processer = DataProcesser(
    "control_run_accept", WEBSHRINKER_CREDENTIALS
)

style_and_fashion_experiment_accept_data_processer.filter_ads(
    non_ads=True, unspecific_ads=True
)
style_and_fashion_experiment_accept_data_processer.update_crawling_data_process()
