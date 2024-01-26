import base64
import hashlib
from urllib.parse import urlencode

import aiohttp


class Categorizer:

    BASE_URL = "https://api.webshrinker.com"

    def __init__(self, api_key: str = None, secret_key: str = None):
        self.api_key = api_key
        self.secret_key = secret_key

    async def categorize(
        self, session: aiohttp.ClientSession, url: str, taxonomy: str = "iabv1"
    ) -> dict:
        """
        Categorizes the given URL using WebShrinker API.

        Parameters:
        - url (str): The URL to be categorized.
        - taxonomy (str): The taxonomy to be used, either "iabv1" or "webshrinker".

        Returns:
        - dict: A dictionary containing category information or an error message.
        """
        if not self.api_key or not self.secret_key:
            raise ValueError("Missing credentials")

        # Encode the URL
        encoded_url = base64.urlsafe_b64encode(url.encode()).decode()

        # Query params dict
        params = {"key": self.api_key, "taxonomy": taxonomy}

        # Create the pre-signed request URL
        request = "categories/v3/{}?{}".format(encoded_url, urlencode(params, True))
        request_to_sign = "{}:{}".format(self.secret_key, request).encode("utf-8")
        signed_request_url = hashlib.md5(request_to_sign).hexdigest()

        api_url = "{}/{}&hash={}".format(self.BASE_URL, request, signed_request_url)

        # Perform the async request with the provided AIOHTTP session for simultaneos requests
        async with session.get(api_url) as response:
            status_code = response.status
            data = await response.json()

            # Handle the possible outcomes
            if status_code == 200:
                categories_response = []
                # Extracting relevant category info and returning
                categories = data["data"][0]["categories"]

                for category_info in categories:
                    # IAB{integer}-{integer} is second level.
                    if "IAB" in category_info["id"] and "-" in category_info["id"]:
                        taxonomy_tier = 2
                    elif "IAB" in category_info["id"]:
                        # IAB{integer} is top level category
                        taxonomy_tier = 1
                    else:
                        # webshrinker has only one level
                        taxonomy_tier = None
                    categories_response.append(
                        {
                            "category": category_info["label"],
                            "taxonomy": taxonomy,
                            "taxonomy_tier": taxonomy_tier,
                            "taxonomy_id": category_info["id"],
                            "confident": category_info["confident"],
                        }
                    )
                return {
                    "status_code": status_code,
                    "categories_response": categories_response,
                }
            elif status_code == 202:
                return {
                    "status_code": status_code,
                    "status": "pending",
                    "message": "Categories are being calculated. Check back soon.",
                }
            else:
                return {
                    "status_code": status_code,
                    "data": data,
                }  # This will return the error message provided by the API or other status
