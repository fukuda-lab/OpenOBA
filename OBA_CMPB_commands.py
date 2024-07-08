import errno
import logging
import os
import time
from datetime import datetime
from urllib.parse import urlparse

from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import Firefox
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from openwpm.commands.types import BaseCommand
from openwpm.commands.browser_commands import (
    tab_restart_browser,
    close_other_windows,
    bot_mitigation,
)
from openwpm.config import BrowserParams, ManagerParams
from openwpm.socket_interface import ClientSocket
from openwpm.storage.storage_controller import DataSocket
from openwpm.storage.storage_providers import TableName

import bannerclick.bannerdetection as bc
import bannerclick.cmpdetection as cd

from bannerclick.config import log_file, MOBILE_AGENT

from oba.ad_extraction import (
    identify_ads_in_dom,
    is_ad_chumbox,
    scroll_page_to_load_ads,
    scroll_to_ad_and_get_validness,
    split_chumbox,
    extract_ad_url_without_click_and_screenshot,
)

from typing import List

from selenium.webdriver.remote.webelement import WebElement


def init(headless, input_file, num_browsers, num_repetitions):
    bc.init(headless, input_file, num_browsers, num_repetitions, web_driver=1)
    cd.init(web_driver=1)


def _start_extension(browser_profile_path, browser_params) -> ClientSocket:
    """Start up the extension
    Blocks until the extension has fully started up
    """
    assert browser_params.browser_id is not None
    elapsed = 0.0
    port = None
    ep_filename = browser_profile_path / "extension_port.txt"
    while elapsed < 5:
        try:
            with open(ep_filename, "rt") as f:
                port = int(f.read().strip())
                break
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
        time.sleep(0.1)
        elapsed += 0.1
    if port is None:
        # try one last time, allowing all exceptions to propagate
        with open(ep_filename, "rt") as f:
            port = int(f.read().strip())

    ep_filename.unlink()

    extension_socket = ClientSocket(serialization="json")
    extension_socket.connect("127.0.0.1", int(port))

    success_filename = browser_profile_path / "OPENWPM_STARTUP_SUCCESS.txt"
    startup_successful = False
    while elapsed < 10:
        if success_filename.exists():
            startup_successful = True
            break
        time.sleep(0.1)
        elapsed += 0.1

    if not startup_successful:
        raise
    success_filename.unlink()
    return extension_socket


class Data:
    url = ""
    ttw = 0
    sql_addr = None
    status = None
    index = None
    banners = []
    banners_data = []
    CMP = {}
    openwpm = True

    @staticmethod
    def save_record_in_sql(table_name, row):
        sock = DataSocket(Data.sql_addr)
        sock.store_record(TableName(table_name), row["visit_id"], row)


class ExtractAdsCommand(BaseCommand):
    """
    Extract the advertisements urls from the current page.
    """

    def __init__(self, url=None, clean_run=False) -> None:
        self.logger = logging.getLogger("openwpm")
        self.url = url
        self.clean_run = clean_run

    def __repr__(self):
        return "ExtractAdsCommand({})".format(self.url)

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        def prepare_screenshot_dir():
            # CREATE DIRECTORY TO SAVE SCREENSHOTS
            parsed_url = urlparse(self.url).netloc.split(".")
            print(parsed_url)
            domain = parsed_url[0]
            print(domain)
            domain_dir = f"./ads_screenshots/{domain}/{self.browser_id}/"

            screenshot_dir = os.path.join(manager_params.data_directory, domain_dir)
            # Create directory if it doesn't exist
            os.makedirs(screenshot_dir, exist_ok=True)

            return screenshot_dir

        try:
            # SCROLL PAGE TO LOAD ADS
            scroll_page_to_load_ads(webdriver)

            # GET ALL ADS ACCORDING TO EASYLIST
            ads = identify_ads_in_dom(webdriver)

            screenshot_dir = prepare_screenshot_dir()

            non_chumbox_ads: List[WebElement] = []
            chumbox_ads: List[WebElement] = []

            for ad in ads:
                if is_ad_chumbox(ad):
                    chumbox_ads.append(ad)
                else:
                    non_chumbox_ads.append(ad)

            print(
                f"Found {len(non_chumbox_ads)} possible non chumbox ads on {self.url}"
            )
            print(f"Found {len(chumbox_ads)} possible chumbox ads on {self.url}")

            ad_number = 0
            # First scrape all visible non-chumbox ads giving less than 0.5 seconds to load each ad (i.e. extract all ads that are already visible on the page)
            for ad_index, ad_element in enumerate(non_chumbox_ads):
                # print(f"Scraping non chumbox ad {ad_index} / {len(non_chumbox_ads)}")
                platform = None
                try:
                    # ad_valid = scroll_to_ad_and_get_validness(
                    #     driver=webdriver, ad=ad_element, wait_time=0.5
                    # )
                    # if ad_valid:
                    # ad_url = extract_ad_url_click_and_screenshot(
                    #     driver=webdriver,
                    #     screenshot_target=ad_element,
                    #     click_target=ad_element,
                    #     screenshot_file_target_path=screenshot_dir,
                    # )
                    # ad_urls.append(ad_url)
                    # else:
                    #     print(f"Non chumbox ad {ad_index} is not valid, skipping...")
                    screenshot_full_path = os.path.join(
                        screenshot_dir, f"{self.visit_id}_{ad_number}.png"
                    )
                    possible_ad_urls = extract_ad_url_without_click_and_screenshot(
                        webdriver=webdriver,
                        ad_element=ad_element,
                        screenshot_target=ad_element,
                        screenshot_file_target_path=screenshot_full_path,
                    )
                    # print(
                    #     f"Captured {len(possible_ad_urls)} possible ad {ad_number} URLs"
                    # )
                    # We will save all possible ad urls as separate ads in the database but we can reference from which "unique" ad it comes according to the ad_number_in_visit
                    Data.sql_addr = manager_params.storage_controller_address
                    for ad_url in possible_ad_urls:
                        Data.save_record_in_sql(
                            TableName("visit_advertisements"),
                            {
                                "visit_id": self.visit_id,
                                "browser_id": self.browser_id,
                                "ad_number_in_visit": ad_number,
                                "ad_url": ad_url,
                                "visit_url": self.url,
                                "clean_run": self.clean_run,
                            },
                        )
                    ad_number += 1
                    # print(possible_ad_urls)
                except Exception as e:
                    print(f"Failed to extract {ad_element}")
                    print(e)

            # After that, scrape chumbox ads
            for chumbox_index, chumbox_ad_element in enumerate(chumbox_ads):
                # print(f"Scraping chumbox ad {chumbox_index} / {len(chumbox_ads)}")
                chumbox = split_chumbox(driver=webdriver, ad_element=chumbox_ad_element)
                platform = chumbox["platform"]
                sub_ads = chumbox["ad_handles"]
                sub_ad_number = 1
                chumbox_ad_counted = False
                for sub_ad_index, sub_ad in enumerate(sub_ads):
                    print(
                        # f"------ Scraping sub_ad {sub_ad_index} / {len(sub_ads)} -------"
                    )
                    try:
                        # sub_ad_valid = scroll_to_ad_and_get_validness(
                        #     driver=webdriver, ad=sub_ad, wait_time=0.5
                        # )
                        # if sub_ad_valid:
                        #     sub_ad_url = extract_ad_url_click_and_screenshot(
                        #         driver=webdriver,
                        #         screenshot_target=ad_element,
                        #         click_target=ad_element,
                        #         screenshot_file_target_path=screenshot_dir,
                        #     )
                        #     ad_urls.append(sub_ad_url)
                        #     print(f"SCRAPED sub ad URL: {sub_ad_url}")
                        # else:
                        #     print(
                        #         f"Non chumbox ad {ad_index} is not valid, skipping..."
                        #     )
                        # chumbox_ads.pop(chumbox_index)
                        ad_element = sub_ad["clickTarget"]
                        screenshot_target = sub_ad["screenshotTarget"]
                        screenshot_full_path = os.path.join(
                            screenshot_dir,
                            f"{self.visit_id}_{ad_number}c{sub_ad_number}.png",
                        )
                        possible_ad_urls = extract_ad_url_without_click_and_screenshot(
                            webdriver=webdriver,
                            ad_element=ad_element,
                            screenshot_target=screenshot_target,
                            screenshot_file_target_path=screenshot_full_path,
                        )
                        for ad_url in possible_ad_urls:
                            Data.save_record_in_sql(
                                TableName("visit_advertisements"),
                                {
                                    "visit_id": self.visit_id,
                                    "browser_id": self.browser_id,
                                    "ad_number_in_visit": ad_number,
                                    "sub_ad_number_in_chumbox": sub_ad_number,
                                    "ad_url": ad_url,
                                    "visit_url": self.url,
                                    "clean_run": self.clean_run,
                                    "chumbox_platform": platform,
                                },
                            )
                        sub_ad_number += 1
                        if not chumbox_ad_counted:
                            ad_number += 1
                            chumbox_ad_counted = True
                        # print(possible_ad_urls)
                    except Exception as e:
                        print(f"Failed to extract sub ad {sub_ad} from ad chumbox")
                        print(e)

        except Exception as e:
            print(f"[ERROR extracting ads from {self.url}]")
            print(e)
            with open(log_file, "a+") as f:
                print(
                    "failed in CMPBCommand for url: " + self.url + " " + e.__str__(),
                    file=f,
                )


class SubGetCommand(BaseCommand):
    """
    goes to <url> using the given <webdriver> instance
    """

    def __init__(self, url, sleep):
        self.url = url
        self.sleep = sleep

    def __repr__(self):
        return "SubGetCommand({},{})".format(self.url, self.sleep)

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        tab_restart_browser(webdriver)

        if extension_socket is not None:
            extension_socket.send(self.visit_id)

        # Execute a get through selenium
        try:
            webdriver.get(self.url)
            Data.status = 0
        except TimeoutException:  # timeout
            Data.status = 1
        except WebDriverException:  # unreachable
            Data.status = 2
            return

        # Sleep after get returns
        time.sleep(self.sleep)

        # Close modal dialog if exists
        try:
            WebDriverWait(webdriver, 0.5).until(EC.alert_is_present())
            alert = webdriver.switch_to.alert
            alert.dismiss()
            time.sleep(1)
        except (TimeoutException, WebDriverException):
            pass

        close_other_windows(webdriver)

        if browser_params.bot_mitigation:
            bot_mitigation(webdriver)


class CMPBCommand(BaseCommand):
    """
    run all the Get, Bannerdetection, CMPDetection and SetEntry Command in one single command.
    """

    def __init__(self, url, sleep, index, timeout, choice):
        self.logger = logging.getLogger("openwpm")
        self.url = url
        self.sleep = sleep
        self.index = index
        self.timeout = timeout
        self.choice = choice

    def __repr__(self):
        return "CMPBCommand({},{},{},{})".format(
            self.url, self.sleep, self.index, self.timeout, self.choice
        )

    def init_data(self):
        Data.url = self.url
        Data.index = self.index
        Data.sleep = self.sleep
        Data.ttw = 0
        Data.btn_status = {"btn_status": None, "btn_set_status": None}
        Data.nc_cmp_name = None
        Data.banners = []
        Data.banners_data = []
        Data.CMP = None
        Data.interact_time = 0
        Data.start_time = datetime.now()
        Data.finish_time = 0

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        # if "https://www.tribunnews.com" == self.url:
        #     i = 0
        tab_restart_browser(webdriver)

        # webdriver = bd.create_driver_session(webdriver.session_id, webdriver.command_executor._url)
        # webdriver = bd.set_webdriver()
        webdriver.set_page_load_timeout(self.timeout)
        error_flag = False
        exception = None
        # webdriver.uninstall_addon('openwpm@mozilla.org')

        bc.set_webdriver(webdriver)
        time.sleep(5)
        # agent = webdriver.execute_script("return navigator.userAgent")
        # print('\n\nagent:  ', agent)
        # print('\n\nsize:  ', webdriver.get_window_size())
        try:
            webdriver.get(self.url)
            Data.status = 0
        except Exception as E:
            try:
                if bc.URL_MODE == 3:
                    raise E
                # self.url = self.url.replace('https', 'http')
                self.url = self.url.replace("://", "://www.")
                webdriver.get(self.url)
                Data.status = 0
            except TimeoutException:  # timeout
                Data.status = 1
            except Exception as E:  # unreachable
                Data.status = 2
                error_flag = True
                exception = E

        #        time.sleep(self.sleep)

        # Close modal dialog if exists
        try:
            WebDriverWait(webdriver, 0.5).until(EC.alert_is_present())
            alert = webdriver.switch_to.alert
            alert.dismiss()
            time.sleep(1)
        except (TimeoutException, WebDriverException):
            pass

        try:
            close_other_windows(webdriver)

            if browser_params.bot_mitigation:
                bot_mitigation(webdriver)
            current_url = webdriver.current_url

            # Don't run banner detection if choice is 0
            self.init_data()
            if not bc.BANNERCLICK:
                time.sleep(self.sleep)

            # # Run banner detection and interaction
            else:
                banners = bc.run_banner_detection(Data)
                Data.banners = banners
                Data.banners_data = bc.extract_banners_data(banners)
                bc.interact_with_banners(Data, self.choice)
                if bc.SLEEP_AFTER_INTERACTION:
                    Data.start_time = datetime.now()
                cd.set_webdriver(webdriver)
                Data.CMP = cd.run_cmp_detection()
                Data.sql_addr = manager_params.storage_controller_address
                bc.set_data_in_db_error(Data)
                if bc.WAITANYWAY or self.choice and banners:
                    bc.halt_for_sleep(Data)
        except Exception as ex:
            with open(log_file, "a+") as f:
                print(
                    "failed in CMPBCommand for url: " + self.url + " " + ex.__str__(),
                    file=f,
                )

        if error_flag:
            raise exception

        if self.choice == 0:
            self.logger.info(
                "CMPB command is successfully executed for {} (without Interaction).".format(
                    current_url
                )
            )
        else:
            self.logger.info(
                "CMPB command is successfully executed and result for {} is: number of"
                " banners {} and CMP existance {}.".format(
                    current_url, len(banners), Data.CMP["__tcfapi"]
                )
            )


class InitCommand(BaseCommand):
    def __init__(self) -> None:
        self.logger = logging.getLogger("openwpm")

    def __repr__(self) -> str:
        return "Init"

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        bc.init(web_driver=1)
        cd.init(web_driver=1)
        self.logger.info("Init command is successfully executed.")


class BannerDetectionCommand(BaseCommand):
    def __init__(self, index) -> None:
        self.logger = logging.getLogger("openwpm")
        # mem = id(Data)
        # pro = os.getpid()
        # th = threading.get_ident()
        self.index = index

    def __repr__(self) -> str:
        return "BannerDetection"

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        current_url = webdriver.current_url
        bc.set_webdriver(webdriver)
        Data.index = self.index
        banners = bc.run_banner_detection(Data)
        Data.banners = banners
        self.logger.info(
            "BannerDetection command is successfully executed and there is %d banners"
            " for: %s",
            len(banners),
            current_url,
        )


class CMPDetectionCommand(BaseCommand):
    def __init__(self) -> None:
        self.logger = logging.getLogger("openwpm")

    def __repr__(self) -> str:
        return "CMPDetection"

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        current_url = webdriver.current_url
        cd.set_webdriver(webdriver)
        CMP = cd.run_cmp_detection()
        Data.CMP = CMP
        self.logger.info(
            "CMPDetection command is successfully executed and result is %s for: %s",
            CMP["__tcfapi"],
            current_url,
        )


class SetEntryCommand(BaseCommand):
    def __init__(self) -> None:
        self.logger = logging.getLogger("openwpm")

    def __repr__(self) -> str:
        return "SetEntry"

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        Data.sql_addr = manager_params.storage_controller_address
        current_url = webdriver.current_url
        bc.set_webdriver(webdriver)
        bc.set_data_in_db(current_url, Data)
        self.logger.info(
            "SetEntry command is successfully executed for: %s", current_url
        )
        if Data.status in [2, 3]:
            raise


class SaveDatabaseCommand(BaseCommand):
    def __init__(self) -> None:
        self.logger = logging.getLogger("openwpm")

    def __repr__(self) -> str:
        return "SaveDatabase"

    def execute(
        self,
        webdriver: Firefox,
        browser_params: BrowserParams,
        manager_params: ManagerParams,
        extension_socket: ClientSocket,
    ) -> None:
        Data.sql_addr = manager_params.storage_controller_address
        dbs_name = ["visits", "banners", "htmls"]
        dbs = bc.get_database()
        for i, db in enumerate(dbs):
            dict_list = db.to_dict("records")
            for row in dict_list:
                Data.save_record_in_sql(TableName(dbs_name[i]), row)
        self.logger.info("SaveDatabase command is successfully executed.")
