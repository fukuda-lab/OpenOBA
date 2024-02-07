from oba_crawler import OBAMeasurementExperiment

# Load experiment instance with tranco params default (cached list of 10000 size)
clothing_cookie_banner_exp = OBAMeasurementExperiment(
    experiment_name="clothing_cookie_banner_presence", fresh_experiment=True
)
print("--------- EXPERIMENT GET COOKIE BANNER PRESENCE DEMO ---------")

# Those pages without cookie_banner_checked
clothing_cookie_banner_exp.set_training_pages_by_category(
    "Style & Fashion", confident=True, size=100, cookie_banner_checked=False
)

print("Pages that haven't been crawled for cookie banner presence yet:")
print(clothing_cookie_banner_exp.training_pages)
print(len(clothing_cookie_banner_exp.training_pages))

# Those pages with check, both found presence and not presence
clothing_cookie_banner_exp.set_training_pages_by_category(
    "Style & Fashion",
    confident=True,
    size=100,
    cookie_banner_checked=True,
    cookie_banner_presence=None,
)

print(
    "Pages that have been crawled for cookie banner (and found with presence or without"
    " presence):"
)

print(clothing_cookie_banner_exp.training_pages)
print(len(clothing_cookie_banner_exp.training_pages))

clothing_cookie_banner_exp.set_training_pages_by_category(
    "Style & Fashion", confident=True, size=100, cookie_banner_checked=False
)
clothing_cookie_banner_exp.run_and_save_bannerclick_finding_on_pages()

print("After running bannerclick findings on pages: ")

# Those pages with check and found presence
clothing_cookie_banner_exp.set_training_pages_by_category(
    "Style & Fashion",
    confident=True,
    size=100,
    cookie_banner_checked=True,
    cookie_banner_presence=True,
)

print("Pages that have been crawled for cookie banner and found with presence:")
print(clothing_cookie_banner_exp.training_pages)
print(len(clothing_cookie_banner_exp.training_pages))

# Those pages with check and found without presence
clothing_cookie_banner_exp.set_training_pages_by_category(
    "Style & Fashion",
    confident=True,
    size=100,
    cookie_banner_checked=True,
    cookie_banner_presence=False,
)

print("Pages that have been crawled for cookie banner and found without presence:")
print(clothing_cookie_banner_exp.training_pages)
print(len(clothing_cookie_banner_exp.training_pages))


# Total pages with check and found presence or not presence
clothing_cookie_banner_exp.set_training_pages_by_category(
    "Style & Fashion",
    confident=True,
    size=100,
    cookie_banner_checked=True,
    cookie_banner_presence=None,
)

print(
    "Pages that have been crawled for cookie banner (and found with presence or without"
    " presence):"
)
print(clothing_cookie_banner_exp.training_pages)
print(len(clothing_cookie_banner_exp.training_pages))
print("These should add up to the total pages of the category (capped at size 100)")

# testing_bannerclick_findings = OBAMeasurementExperiment(
#     experiment_name="testing_bannerclick_crawl", fresh_experiment=True
# )

# testing_bannerclick_findings.run_and_save_bannerclick_finding_on_pages(
#     page_urls=["http://adidas.com"]
# )
