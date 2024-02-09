# sys.path.append("../openwpm")
import random
import sys
from typing import Callable, List
from urllib.parse import urlparse

# from bannerclick.config import TIME_OUT
from CMPB_commands import CMPBCommand

# CustomBannerInteraction
from openwpm.command_sequence import (
    CommandSequence,
    RecursiveDumpPageSourceCommand,
    ScreenshotFullPageCommand,
)
from openwpm.commands.browser_commands import GetCommand

TIME_OUT = 60  # OpenWPM timeout = TIME_OUT*11, Selenium timeout = TIME_OUT
TESTING = False
CONTROL_SITE_WAIT_REF = 120
TRAINING_SITE_WAIT_REF = 90


def control_site_visit_sequence(
    control_site: str,
    next_site_rank: str = 0,
    clean_run: bool = False,
    cookie_banner_action: int = 0,
):
    """Returns a command sequence that makes a clean run for a given control_site"""

    def _control_sites_callback(success: bool, val: str = control_site) -> None:
        print(
            f"{'[CLEAN VISIT]' if clean_run else '[CONTROL VISIT]'} for {val} ran"
            f" {'SUCCESSFULLY' if success else 'UNSUCCESSFULLY'}"
        )

    control_site_sequence = CommandSequence(
        control_site,
        site_rank=next_site_rank,
        callback=_control_sites_callback,
        reset=clean_run,
    )
    domain = urlparse(control_site).netloc.split(".")[0]
    wait_time = random.randint(int(CONTROL_SITE_WAIT_REF / 2), CONTROL_SITE_WAIT_REF)
    if cookie_banner_action == 0:
        control_site_sequence.append_command(
            GetCommand(control_site, sleep=wait_time if not clean_run else 10),
            timeout=(
                wait_time + 2 * CONTROL_SITE_WAIT_REF
                if not clean_run
                else TIME_OUT * 11
            ),
        )
    else:
        control_site_sequence.append_command(
            CMPBCommand(
                control_site,
                sleep=wait_time if not TESTING else 5,
                timeout=(
                    wait_time + 2 * CONTROL_SITE_WAIT_REF if not TESTING else TIME_OUT
                ),
                index=next_site_rank,
                choice=cookie_banner_action,
                # result_csv_file_name=banner_results_csv_name,
            ),
            timeout=(
                wait_time + (CONTROL_SITE_WAIT_REF + TIME_OUT) * 11
                if not TESTING
                else TIME_OUT * 11
            ),
        )
    if clean_run:
        sc_suffix = "_C_"
    else:
        sc_suffix = "_"
    control_site_sequence.append_command(
        # ScreenshotFullPageCommand("_"), timeout=wait_time
        ScreenshotFullPageCommand(sc_suffix),
        timeout=TIME_OUT * 11,
    )
    control_site_sequence.append_command(
        # RecursiveDumpPageSourceCommand(domain), timeout=wait_time
        RecursiveDumpPageSourceCommand(domain),
        timeout=TIME_OUT * 11,
    )

    return control_site_sequence


def individual_training_visit_sequence(
    training_site: str,
    next_site_rank=None,
    sleep: int = 10,
    # Run for creating the experiment browser profile and directories
    creation: bool = False,
    cookie_banner_action: int = 0,
):
    """Visits one training_site"""

    def _training_sites_callback(success: bool, val: str = training_site) -> None:
        print(
            f"{'[CREATION VISIT]' if creation else '[TRAINING VISIT]'} for {val} ran"
            f" {'SUCCESSFULLY' if success else 'UNSUCCESSFULLY'}"
        )

    training_visit_sequence = CommandSequence(
        training_site,
        callback=_training_sites_callback,
    )

    if creation or cookie_banner_action == 0:
        training_visit_sequence.append_command(
            GetCommand(training_site, sleep=sleep if not creation else 1),
            timeout=sleep + TIME_OUT if not creation else TIME_OUT,
        )
    # TODO: Probar con distintas choices
    else:
        training_visit_sequence.append_command(
            CMPBCommand(
                training_site,
                sleep=sleep if not TESTING else 5,
                index=next_site_rank,
                timeout=sleep + TIME_OUT if not TESTING else TIME_OUT,
                choice=cookie_banner_action,
                # result_csv_file_name=banner_results_csv_name,
            ),
            timeout=sleep + TIME_OUT * 10 if not TESTING else TIME_OUT * 11,
        )

    return training_visit_sequence


def training_visits_sequence(
    training_sites: List[str],
    next_site_rank: int,
    cookie_banner_action: int = 0,
):
    command_sequences = []
    sites_remaining = training_sites.copy()

    while sites_remaining:
        # Pick a random element from the list and pop it
        random_index = random.randint(0, len(sites_remaining) - 1)
        site_to_visit = sites_remaining.pop(random_index)

        # Exponential distribution with mean 180 segs
        # wait_time = int(random.expovariate(1 / 180))
        wait_time = random.randint(
            int(TRAINING_SITE_WAIT_REF / 2), TRAINING_SITE_WAIT_REF
        )

        command_sequences.append(
            individual_training_visit_sequence(
                site_to_visit,
                next_site_rank,
                wait_time,
                # banner_results_csv_name=banner_results_csv_name,
                cookie_banner_action=cookie_banner_action,
            )
        )
        next_site_rank += 1
        # For now, for testing only
        # command_sequences.append(individual_training_visit_sequence(site_to_visit, 30))

    return command_sequences


def get_cookie_banner_visit_sequences(
    training_pages: list,
    control_pages: list = [],
    sleep_time: int = 5,
):
    """One by one, visits all of the training pages and control pages."""
    command_sequences = []
    sites_remaining = training_pages.copy()
    sites_remaining.extend(control_pages.copy())
    total_sites = len(sites_remaining)

    while sites_remaining:
        # Pick a random element from the list and pop it
        random_index = random.randint(0, len(sites_remaining) - 1)
        site_to_visit = sites_remaining.pop(random_index)

        def _visit_callback(
            success: bool,
            val: str = site_to_visit,
            site_rank: int = total_sites - len(sites_remaining) + 1,
        ) -> None:
            print(
                f"{f'[REJECT COOKIES VISIT {site_rank}]'} for {val} ran"
                f" {'SUCCESSFULLY' if success else 'UNSUCCESSFULLY'}"
            )

        next_site_rank = total_sites - len(sites_remaining) + 1
        visit_sequence = CommandSequence(
            site_to_visit,
            # Starts from 1 to total_sites
            site_rank=next_site_rank,
            callback=_visit_callback,
        )

        visit_sequence.append_command(
            CMPBCommand(
                site_to_visit,
                index=next_site_rank,
                sleep=sleep_time,
                # timeout=time_for_user + 120,
                timeout=TIME_OUT,
                choice=2,
                # result_csv_file_name=banner_results_csv_name,
            ),
            # timeout=time_for_user + 180,
            timeout=TIME_OUT * 11,
        )

        command_sequences.append(visit_sequence)

    return command_sequences
