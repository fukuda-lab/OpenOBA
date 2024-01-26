# The file was run from the fobam directory

from oba_crawler import OBAMeasurementExperiment

# We use the OBAMeasurementExperiment class to access the training pages handler
experiment = OBAMeasurementExperiment("training_pages_handler", True)

# We get the most popular pages by category that were confidently classified
popular_sites_dict = (
    experiment.training_pages_handler.get_most_popular_pages_by_category(
        k=20, confident=True
    )
)

# We get the page urls from the categories we want to know if they have cookie banners
confident_sites = (
    popular_sites_dict["Fashion"]["page_urls"]
    + popular_sites_dict["Buying / Selling Homes"]["page_urls"]
    + popular_sites_dict["Air Travel"]["page_urls"]
    + popular_sites_dict["Health & Fitness"]["page_urls"]
    + popular_sites_dict["Politics"]["page_urls"]
    + popular_sites_dict["Shopping"]["page_urls"]
    + popular_sites_dict["Sports"]["page_urls"]
    + popular_sites_dict["Video & Computer Games"]["page_urls"]
    + popular_sites_dict["Movies"]["page_urls"]
    + popular_sites_dict["Job Search"]["page_urls"]
    + popular_sites_dict["Education"]["page_urls"]
    + popular_sites_dict["Tennis"]["page_urls"]
    + popular_sites_dict["Accessories"]["page_urls"]
    + popular_sites_dict["Books & Literature"]["page_urls"]
    + popular_sites_dict["Dating / Personals"]["page_urls"]
    + popular_sites_dict["Gambling"]["page_urls"]
    + popular_sites_dict["Food & Drink"]["page_urls"]
    + popular_sites_dict["Marketing"]["page_urls"]
    + popular_sites_dict["Job Search"]["page_urls"]
    + popular_sites_dict["Pets"]["page_urls"]
    + popular_sites_dict["Photography"]["page_urls"]
    + popular_sites_dict["Music & Audio"]["page_urls"]
)

# We look for cookie banners on the pages and save in the training pages db (default)
experiment.write_bannerclick_finding_on_pages(confident_sites)
