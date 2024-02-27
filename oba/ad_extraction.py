from datetime import datetime
import os
import random
import time
from typing import Dict, List
import uuid
from selenium.webdriver.common.by import By
from selenium.webdriver import Firefox
from selenium.webdriver.remote.webelement import WebElement
from .resources.easylist_selectors import EASYLIST_SELECTORS

# Import TimeoutException and WebDriverException from selenium.common.exceptions
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

CHUMBOX_DEFINITIONS = [
    {
        "platform": "adblade",
        "selector": ".adblade-dyna a.description",
        "screenshotParentDepth": 2,
    },
    {"platform": "contentad", "selector": ".ac_container", "screenshotParentDepth": 0},
    {"platform": "feednetwork", "selector": ".my6_item", "screenshotParentDepth": 0},
    {"platform": "mgid", "selector": ".mgline", "screenshotParentDepth": 0},
    {
        "platform": "outbrain",
        "selector": ".ob-dynamic-rec-container.ob-p",
        "screenshotParentDepth": 0,
    },
    {"platform": "revcontent", "selector": ".rc-item", "screenshotParentDepth": 0},
    {
        "platform": "taboola",
        "selector": ".trc_spotlight_item.syndicatedItem",
        "screenshotParentDepth": 0,
    },
    {"platform": "zergnet", "selector": ".zergentity", "screenshotParentDepth": 0},
]


def scroll_page_to_load_ads(driver: Firefox, timeout=30):
    """
    Scrolls through the webpage to ensure all dynamically loaded ads are visible in the DOM.

    :param driver: The Selenium WebDriver instance.
    :param timeout: Maximum time to wait for ads to load after each scroll.
    """
    screen_height = driver.execute_script("return window.innerHeight;")
    i = 0.5
    total_sleep_time = 0

    while True:
        # Scroll by one screen height each time
        driver.execute_script(f"window.scrollTo(0, {screen_height}*{i});")
        i += 0.5

        # Random float sleep time between 0.5 and 2.5 seconds
        scroll_pause_time = 0.5 + (3.5) * random.random()
        time.sleep(scroll_pause_time)
        total_sleep_time += scroll_pause_time

        # Check if the bottom of the page is reached
        scroll_height = driver.execute_script("return document.body.scrollHeight;")
        if (screen_height * i) > scroll_height or total_sleep_time > timeout:
            break  # Exit the loop when the end of the page is reached

    print(f"Scrolled to the bottom of the page in {total_sleep_time} seconds")
    if total_sleep_time < timeout:
        remaining_timeout = timeout - total_sleep_time
        try:
            # Wait for dynamic content to load
            WebDriverWait(driver, remaining_timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            print(
                "Timed out waiting for page to load completely, continuing with what has loaded."
            )


def identify_ads_in_dom(driver: Firefox) -> list[WebElement]:
    ads = []
    # Assuming `easylist` is a list of CSS selectors for ads
    for selector in EASYLIST_SELECTORS:
        ads.extend(driver.find_elements(By.CSS_SELECTOR, selector))
    return ads  # This list contains Selenium WebElement objects representing ads


def is_ad_chumbox(ad_element: WebElement) -> bool:
    """
    Identifies if an ad element is a chumbox.

    :param ad_element: The WebElement representing a potential chumbox ad.
    :return: True if chumbox, else False.
    """
    for definition in CHUMBOX_DEFINITIONS:
        if ad_element.find_elements(By.CSS_SELECTOR, definition["selector"]):
            return True
    return False


def scroll_to_ad_and_get_validness(driver: Firefox, ad: WebElement, wait_time: float):
    # Apply same conditionals as adscraper in puppeteer
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center'});",
        ad,
    )
    # wait a random float around wait_time seconds
    delta_time = wait_time / 4 * random.random()
    time.sleep(wait_time + -delta_time)

    if ad.size["height"] < 30 or ad.size["width"] < 30:
        print("Ad is smaller than 30px in one dimension, skipping...")
        return False  # Skip this ad

    viewport_size = driver.get_window_size()
    if not viewport_size:
        print("Viewport size not found, skipping...")
        return False  # Skip this ad

    if not ad.is_displayed():
        print("Ad is not displayed, skipping...")
        return False

    return True  # This ad is valid


def split_chumbox(
    driver, ad_element: WebElement
) -> Dict[str, List[Dict[str, WebElement]]]:
    """
    splits chumbox it into individual ads.

    :param driver: The Selenium WebDriver instance.
    :param ad_element: The WebElement representing a potential chumbox ad.
    :param chumbox_definitions: Definitions for identifying chumbox ads.
    :return: A dictionary with platform and ad_handles if chumbox, else only ad_handle.
    """
    default_ad_handle = {"clickTarget": ad_element, "screenshotTarget": ad_element}
    WebDriverWait(driver, 10).until(EC.visibility_of(ad_element))

    for definition in CHUMBOX_DEFINITIONS:
        # Attempt to find sub-ads using the provided selector
        sub_ads = ad_element.find_elements(By.CSS_SELECTOR, definition["selector"])
        if sub_ads:
            ad_handles = []
            for sub_ad in sub_ads:
                try:
                    WebDriverWait(driver, 10).until(EC.visibility_of(sub_ad))
                    screenshot_target = sub_ad
                    for _ in range(definition["screenshotParentDepth"]):
                        # Navigate up the DOM to the appropriate parent element
                        screenshot_target = screenshot_target.find_element(
                            By.XPATH, ".."
                        )

                    ad_handles.append(
                        {
                            "clickTarget": sub_ad,
                            "screenshotTarget": screenshot_target,
                        }
                    )
                except StaleElementReferenceException:
                    print(
                        f"[chumbox split] StaleElementReferenceException: Sub-Ad not found in the DOM"
                    )
            return {
                "platform": definition["platform"],
                "ad_handles": ad_handles if ad_handles else [default_ad_handle],
            }

    # No chumbox found of , treat the whole ad as both clickTarget and screenshotTarget
    return {
        "platform": None,
        "ad_handles": [default_ad_handle],
    }


def extract_ad_url_without_click_and_screenshot(
    webdriver: Firefox,
    ad_element: WebElement,
    screenshot_target: WebElement,
    screenshot_file_target_path: str,
):
    found_urls = []

    # Function to execute a script and return links
    def execute_script_and_get_links(script, *args):
        try:
            return webdriver.execute_script(script, *args)
        except Exception as e:
            # print(f"Error executing script: {e}")
            return []

    # Extract links directly within the ad_element
    direct_links_script = """
        var links = [];
        var elements = arguments[0].querySelectorAll('a');
        elements.forEach(function(element) {
            var href = element.href;
            if (href) links.push(href);
        });
        return links;
    """
    direct_links = execute_script_and_get_links(direct_links_script, ad_element)
    found_urls.extend(direct_links)

    # Function to recursively find links within iframes
    def find_links_in_iframes(iframe_elements):
        for iframe in iframe_elements:
            try:
                webdriver.switch_to.frame(iframe)
                # Now that we're inside the iframe, look for links directly in this context
                links_in_iframe = execute_script_and_get_links(
                    """
                    var links = [];
                    document.querySelectorAll('a').forEach(function(element) {
                        var href = element.href;
                        if (href) links.push(href);
                    });
                    return links;
                """
                )
                found_urls.extend(links_in_iframe)

                # Look for nested iframes recursively
                nested_iframes = webdriver.find_elements(By.TAG_NAME, "iframe")
                find_links_in_iframes(nested_iframes)

                webdriver.switch_to.parent_frame()
            except Exception as e:
                # print(f"Error processing iframe: {e}")
                webdriver.switch_to.parent_frame()

    # Start by looking for iframes within the ad_element
    iframes_in_ad = ad_element.find_elements(By.TAG_NAME, "iframe")
    find_links_in_iframes(iframes_in_ad)

    # Ensure we're back at the top-level context after all processing
    webdriver.switch_to.default_content()

    try:
        # Take screenshot
        screenshot_target.screenshot(screenshot_file_target_path)
        return found_urls
    except WebDriverException as e:
        # print("WebDriverException taking screenshot of ad")
        # print(e)
        return found_urls


def _old_extract_ad_url_click_and_screenshot(
    driver: Firefox,
    screenshot_target: WebElement,
    click_target: WebElement,
    screenshot_file_target_path: str,
):
    """This function was changed with extract without clicking"""

    def _click_ad_and_get_url(webdriver: Firefox, ad_handle: WebElement):
        original_url = webdriver.current_url
        main_window = webdriver.current_window_handle
        # Click the ad, which is expected to open in a new tab
        ad_handle.click()
        driver.execute_script("arguments[0].click();", ad_handle)
        time.sleep(1)  # Wait a fraction of a second for any new tab to be opened

        # Handle the new tab if it was opened
        if len(webdriver.window_handles) > 1:
            # Identify the new tab and switch to it
            new_tab = [tab for tab in webdriver.window_handles if tab != main_window][0]
            webdriver.switch_to.window(new_tab)

            # Capture the URL of the new tab
            ad_url = webdriver.current_url

            # Close the new tab and switch back to the main window
            webdriver.close()
            webdriver.switch_to.window(main_window)
            return ad_url
        elif webdriver.current_url != original_url:
            # If the ad opened in the same tab, print the URL and also print a warning
            raise Exception(f"CurrentURLChanged from: {ad_url}")

        else:
            # Apparently the click did nothing, raise an exception
            raise Exception("Ad click did nothing")

    # Click ad and save ad URL
    try:
        ad_url = _click_ad_and_get_url(driver, click_target)
        # print(f"Scraped ad URL: {ad_url}")
    except WebDriverException as e:
        # print("WebException during clicking ad, skipping...")
        raise e
    except Exception as e:
        # print("Normal Exception during clicking ad, skipping...")
        raise e

    try:
        # Take screenshot
        screenshot_target.screenshot(screenshot_file_target_path)
        return ad_url
    except WebDriverException as e:
        # print("WebDriverException taking screenshot of ad, skipping...")
        raise e
