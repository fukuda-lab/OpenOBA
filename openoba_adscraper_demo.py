import argparse
from pathlib import Path
import time
from typing import Literal

from openwpm.command_sequence import CommandSequence
from openwpm.commands.browser_commands import (
    ScreenshotFullPageCommand,
)
from openwpm.config import BrowserParams, ManagerParams
from openwpm.storage.sql_provider import SQLiteStorageProvider
from openwpm.task_manager import TaskManager
from OBA_CMPB_commands import ExtractAdsCommand, CMPBCommand

# THESE SITES CAN BE CHANGED TO ANY LIST OF SITES THAT INCORPORATES ADS
sites = [
    "http://myforecast.com/",
    "http://weatherbase.com/",
    "http://theweathernetwork.com/",
    "http://weather.com/",
    "http://weather2umbrella.com/",
]


display_mode: Literal["native", "headless", "xvfb"] = "headless"
# display_mode = "headless"

# Loads the default ManagerParams
# and NUM_BROWSERS copies of the default BrowserParams
NUM_BROWSERS = 1
manager_params = ManagerParams(num_browsers=NUM_BROWSERS)
browser_params = [BrowserParams(display_mode=display_mode) for _ in range(NUM_BROWSERS)]

# Update browser configuration (use this for per-browser settings)
for browser_param in browser_params:
    # Record HTTP Requests and Responses
    browser_param.http_instrument = True
    # Record cookie changes
    browser_param.cookie_instrument = True
    # Record Navigations
    browser_param.navigation_instrument = True
    # Record JS Web API calls
    browser_param.js_instrument = True
    # Record the callstack of all WebRequests made
    # browser_param.callstack_instrument = True
    # Record DNS resolution
    browser_param.dns_instrument = True
    # Set this value as appropriate for the size of your temp directory
    # if you are running out of space
    # browser_param.maximum_profile_size = 50 * (10**20)  # 50 MB = 50 * 2^20 Bytes

# Update TaskManager configuration (use this for crawl-wide settings)
manager_params.data_directory = Path("./datadir/demo/")
manager_params.log_path = Path("./datadir/demo/openwpm.log")

# memory_watchdog and process_watchdog are useful for large scale cloud crawls.
# Please refer to docs/Configuration.md#platform-configuration-options for more information
# manager_params.memory_watchdog = True
# manager_params.process_watchdog = True


# Commands time out by default after 60 seconds
with TaskManager(
    manager_params,
    browser_params,
    SQLiteStorageProvider(Path("./datadir/demo/crawl-data.sqlite")),
    None,
) as manager:

    for index, site in enumerate(sites):

        def callback(success: bool, val: str = site) -> None:
            print(
                f"CommandSequence for {val} ran {'successfully' if success else 'unsuccessfully'}"
            )

        # Parallelize sites over all number of browsers set above.
        command_sequence = CommandSequence(
            site,
            site_rank=index + 1,
            callback=callback,
        )

        # Start by visiting the page
        command_sequence.append_command(
            CMPBCommand(
                site,
                sleep=10,
                timeout=60,
                index=index + 1,
                choice=1,
            ),
            timeout=(60 * 11),
        )
        # Have a look at custom_command.py to see how to implement your own command
        command_sequence.append_command(ExtractAdsCommand(url=site), timeout=300)

        # command_sequence.append_command(RecursiveDumpPageSourceCommand(suffix="N"), timeout=60)
        command_sequence.append_command(
            ScreenshotFullPageCommand(suffix="N"), timeout=60
        )

        # Run commands across all browsers (simple parallelization)
        manager.execute_command_sequence(command_sequence)
