experiment_config_format = {
    "experiment_name": "test1",
    "pages_categorized": True,  # If False, we use the custom_pages_list in stead of the 'training_pages_id' when loading
    "custom_pages_list": [],
    "training_pages_id": "X53KN",
    "n_pages": 10,
    "browser_ids": {"oba": [], "clear": []},
}

oba_crawler_params_load_exp = {
    "experiment_name": "existent_exp",
    "fresh_experiment": False,
    # Nothing else is required because it was already set up.
}

# DEFAULT VALUES
oba_crawler_params_tranco_pages_cached = {
    "experiment_name": "new_tranco_exp_cached",
    "fresh_experiment": True,  # If True we need the training_pages_params (experiment setup), create browser profile and {experiment_name}_config.json file. If false we don't need anything else.
    "use_custom_pages": False,  # If false we will use tranco_params
    "tranco_pages_params": {
        "updated": False,  # We can used cached N-list already categorized. We have list_id with this.
    },
    "webshrinker_credentials": None,  # Not needed for this case
    "custom_pages_params": None,  # Not needed for this case
}

# REQUIRES CATEGORIZATION OF PAGES
oba_crawler_params_tranco_pages_updated = {
    "experiment_name": "new_tranco_exp_updated",
    "fresh_experiment": True,  # If True we need the training_pages_params (experiment setup), create browser profile and {experiment_name}_config.json file. If false we don't need anything else.
    "use_custom_pages": False,  # If false we will use tranco_params
    "tranco_pages_params": {
        "updated": True,  #  We have to retrieve the last list of tranco and categorize it.
        "size": None,  # Pass this value as n_pages if it is an integer.
    },
    # If use_custom_pages==False and tranco_pages_params['updated'] == True, we need 'webshrinker_credentials'.
    "webshrinker_credentials": {"api_key": "api-key", "secret_key": "secret-key"},
    "custom_pages_params": None,  # Not needed for this case
}

# DOES NOT REQUIRE CATEGORIZATION, DOES NOT USE CATEGORIZATION, THIS JUST LETS THE USER TRAIN A BROWSER PROFILE WITH THE PAGES GIVEN.
oba_crawler_params_custom_training_pages_without_categorize = {
    "experiment_name": "new_custom_exp_no_categorization",
    "fresh_experiment": True,  # If True we need the training_pages_params (experiment setup), create browser profile and {experiment_name}_config.json file. If false we don't need anything else.
    "use_custom_pages": True,  # If True we will use custom_pages_params
    "custom_pages_params": {
        "categorize_pages": False,  # If it is False, we use the custom_pages_list and we do not use a TrainingPagesHandler at all, the list must be then saved in the {experiment_name}_config.json file without a list_id.
        "custom_pages_list": [
            "url1.com",
            "url2.com",
            "url3.com",
        ],  # List of URLs provided by the user.
    },
    "webshrinker_credentials": None,  # Not required for this case
    "tranco_pages_params": None,  # Not required for this case
}

# REQUIRES CATEGORIZATION.
oba_crawler_params_custom_training_pages_with_categorize = {
    "experiment_name": "new_custom_exp_categorize",
    "fresh_experiment": True,  # If True we need the training_pages_params (experiment setup), create browser profile and {experiment_name}_config.json file. If false we don't need anything else.
    "use_custom_pages": True,  # If True we will use custom_pages_params
    "custom_pages_params": {
        "categorize_pages": True,  # If True we load the TrainingPagesHandler with the custom_pages_list=custom_pages_list, under custom_list=True, list_id={experiment_name}.
        "custom_pages_list": [
            "url1.com",
            "url2.com",
            "url3.com",
        ],  # List of URLs provided by the user. This list is not saved in the {experiment_name}_config.json, since it has to first be categorized and then accessed through the TrainingPagesHandler().get_training_pages_by_category() method.
    },
    # If use_custom_pages==True and custom_pages_params['categorize_pages'] == True, we need 'webshrinker_credentials'.
    "webshrinker_credentials": {"api_key": "api-key", "secret_key": "secret-key"},
    "tranco_pages_params": None,  # Not required for this case
}
