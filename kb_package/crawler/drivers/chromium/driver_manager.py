import os

from kb_package.crawler.drivers.chrome.driver_manager import DriverManager as ChromeDriverManager
from kb_package import tools


class DriverManager(ChromeDriverManager):
    NAME = "chrome"
    TRUTH_NAME = "Chromium"

    def get_options(self, **kwargs):
        if "binary_location" not in kwargs:
            raise TypeError("binary_location arg is necessary to use Chromium webdriver")

        options = super().get_options(**kwargs)
        # Useful for auto_exe_path, to find Chromium version
        self.CHROME_BINARY = kwargs["binary_location"]
        path = tools.search_file("chrome.exe",
                                 from_path=os.path.dirname(self.CHROME_BINARY),
                                 depth=3)
        if path is not None:
            self.CHROME_BINARY = path
            # kwargs["binary_location"] = path
        # options = super().get_options(**kwargs)
        return options
