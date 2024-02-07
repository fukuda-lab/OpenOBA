# The file was run from the fobam directory

from oba_crawler import OBAMeasurementExperiment

# We use the OBAMeasurementExperiment class to access the training pages handler
experiment = OBAMeasurementExperiment("training_pages_handler_3", True)

# We get the most popular pages by category that were confidently classified
popular_sites_dict = (
    experiment.training_pages_handler.get_training_pages_grouped_by_category(
        k=50, confident=True, cookie_banner_checked=True, cookie_banner_presence=True
    )
)

# We get the page urls from the categories we want to know if they have cookie banners
confident_sites = (
    popular_sites_dict["Style & Fashion"]["pages_urls"]
    # popular_sites_dict["Clothing"]["pages_urls"]
    # + popular_sites_dict["Fashion"]["pages_urls"]
    # + popular_sites_dict["Buying / Selling Homes"]["pages_urls"]
    # + popular_sites_dict["Air Travel"]["pages_urls"]
    # + popular_sites_dict["Health & Fitness"]["pages_urls"]
    # + popular_sites_dict["Politics"]["pages_urls"]
    # + popular_sites_dict["Shopping"]["pages_urls"]
    # + popular_sites_dict["Sports"]["pages_urls"]
    # + popular_sites_dict["Video & Computer Games"]["pages_urls"]
    # + popular_sites_dict["Movies"]["pages_urls"]
    # + popular_sites_dict["Job Search"]["pages_urls"]
    # + popular_sites_dict["Education"]["pages_urls"]
    # + popular_sites_dict["Tennis"]["pages_urls"]
    # + popular_sites_dict["Accessories"]["pages_urls"]
    # + popular_sites_dict["Books & Literature"]["pages_urls"]
    # + popular_sites_dict["Dating / Personals"]["pages_urls"]
    # + popular_sites_dict["Gambling"]["pages_urls"]
    # + popular_sites_dict["Food & Drink"]["pages_urls"]
    # + popular_sites_dict["Marketing"]["pages_urls"]
    # + popular_sites_dict["Job Search"]["pages_urls"]
    # + popular_sites_dict["Pets"]["pages_urls"]
    # + popular_sites_dict["Photography"]["pages_urls"]
    # + popular_sites_dict["Music & Audio"]["pages_urls"]
)

# print(confident_sites)

# experiment.update_training_pages_db_with_cookie_banner_presence(confident_sites)
# We look for cookie banners on the pages and save in the training pages db (default)
# experiment.run_and_save_bannerclick_finding_on_pages(confident_sites)
