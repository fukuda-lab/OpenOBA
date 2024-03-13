class OBABrowserQueries:
    """They need an instance to set the browser_ids first"""

    def __init__(self, browser_id):
        self.browser_id = browser_id

    def get_visit_rows_per_control_site_query(self, control_site_url: str):
        """Given a control site url, returns a list of tuples with (site_url, visit_id);"""
        return (
            "SELECT site_url, visit_id, site_rank FROM site_visits WHERE browser_id ="
            f" {self.browser_id} AND site_url = '{control_site_url}' ORDER BY"
            " site_rank;"
        )

    def get_unresolved_advertisements_query(self):
        """Returns a list of tuples with (ad_id, landing_page_url)"""
        return f"SELECT ad_id, ad_url FROM visit_advertisements WHERE browser_id = {self.browser_id} AND landing_page_url IS NULL AND non_ad IS NULL AND unspecific_ad IS NULL"


class VisitAdvertisementsQueries:
    """Queries for the visit_advertisements tables"""

    SelectAllAdIdsWithLandingPageURLQuery = "SELECT ad_id FROM visit_advertisements WHERE landing_page_url=:landing_page_url"
    # SelectResolvedAdvertisementsNotCategorizedFromVisitQuery = (
    #     "SELECT landing_page_id, landing_page_url FROM visit_advertisements WHERE visit_id=:visit_id"
    #     " AND browser_id=:browser_id AND landing_page_url IS NOT NULL"
    # )
    SelectResolvedLandingPagesFromADURLQuery = (
        "SELECT landing_page_id, landing_page_url FROM visit_advertisements WHERE ad_url=:ad_url"
        " AND landing_page_url IS NOT NULL"
    )

    UpdateVisitAdvertisementLandingPageQuery = "UPDATE visit_advertisements SET landing_page_url=:landing_page_url, landing_page_id=:landing_page_id, categorized=:categorized WHERE ad_url=:ad_url AND landing_page_url is NULL"


class LandingPagesQueries:
    """Queries for the landing_pages table"""

    InsertLandingPageQuery = (
        "INSERT INTO landing_pages (landing_page_url, categorized) VALUES (?, ?)"
    )
    SelectLandingPageFromURLQuery = "SELECT landing_page_id, landing_page_url, categorized FROM landing_pages WHERE landing_page_url=?"

    UpdateLandingPageCategorizedQuery = "UPDATE landing_pages SET categorized=:categorized WHERE landing_page_id=:landing_page_id"


class LandingPageCategoriesQueries:
    """Queries for the landing_page_categories tables"""

    InsertCategoryQuery = "INSERT INTO landing_page_categories (landing_page_id, landing_page_url, category_name, category_code, parent_category, confident) VALUES (:landing_page_id, :landing_page_url, :category_name, :category_code, :parent_category, :confident)"

    SelectCategoriesFromLandingPageURLQuery = "SELECT category_name, category_code, parent_category, confident FROM landing_page_categories WHERE landing_page_url=:landing_page_url"
