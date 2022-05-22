# -*- coding: utf-8 -*-
"""
Base step objects
"""

import time

from kb_package.crawler.navigation_errors.error_handler import (
    ErrorHandler
)


class Step:
    def __init__(self, driver):
        """
        Constructor
        Args:
            driver: selenuim.webdriver | CustomDriver object
        """
        self._driver = driver

    def execute(self, *args, **kargs):
        """
        Use to run the step
        Args:
            *args:
            **kargs:

        Returns:

        """
        return True

    @property
    def is_applicable(self):
        """
        to know if the step can be execute
        Returns:
            bool, is applicable ?
        """
        return True

    def sleep_until_load(
            self, max_try=3,
            css_selector="img[src], a[href], h1,h2,h3,h4,h5,h6",
            check_error=True, get_data=False
    ):
        """
        Wait for dom load
        Args:
            max_try: int, time in second for wait for dom loaded
            css_selector: str, criteria for dom loaded judgment
            get_data: bool, get the value returns by css_selector
            check_error: bool, if it necessary to check any error in the dom

        Returns:
            dict, signature {"got", "error", "elapsed"}
        """
        start_time = time.time()
        error = None
        data = None
        got = False
        for i in range(max_try):
            try:
                data = self._driver.execute_script(
                    """
                let elm=document.querySelector(arguments[0]);
                return (elm==null)? null: elm.innerText || elm.value || true;""",
                    css_selector,
                )

                if data is not None:
                    got = True
                    break
                if check_error:
                    error = ErrorHandler.detect_error(
                        self._driver.page_source, check_dom_load=False
                    )
                    if error is not None:
                        got = False
                        break
            except Exception as ex:
                got = False
                error = str(ex)
                break
            time.sleep(1)
        d = {
            "got": got,
            "elapsed": time.time() - start_time,
            "error": error
        }
        if not got and error is None:
            d["error"] = "WaitingTimeOut"
        if get_data:
            d["data"] = data
        return d
