import os

from typing import Union

from selenium import webdriver

webdriver.OperaOptions = webdriver.ChromeOptions


class DriverManager:
    NAME = None
    PATH = os.path.dirname(__file__)
    PATH_TO_REF = None

    @property
    def find_navigator_version(self):
        return None

    def get_default_desired_capabilities(self, **kwargs):
        desired_capabilities = kwargs.get("desired_capability", None)

        if desired_capabilities is None:
            desired_capabilities = getattr(
                webdriver.DesiredCapabilities, self.NAME.upper()
            )
            desired_capabilities["acceptSslCerts"] = True
            desired_capabilities["acceptInsecureCerts"] = True

        return desired_capabilities

    def generate_webdriver_exe(self, n_version: Union[int, str] = None):
        yield None

    def install(self, n_version: Union[int, str] = None):
        exe_path = self.generate_webdriver_exe(n_version=n_version).__next__()
        assert exe_path is not None, f"Fail to install " + \
               (f'[for version {n_version}]' if n_version is not None else '')
        return exe_path

    def get_default_profile(self, **kwargs):
        profile = None
        if str(self.NAME).lower() == "firefox":
            profile = webdriver.FirefoxProfile()
        return profile

    def extra_args(self, **kwargs):
        return {}

    def get_default_options(self, **kwargs):
        headless = kwargs.get("headless", False)
        # calculation of option
        options = kwargs.get("driver_options", None)
        binary_location = kwargs.get("binary_location", None)
        if binary_location is not None:
            options.binary_location = binary_location
        options_class = getattr(
                webdriver, self.NAME.capitalize() + "Options"
            )
        if not isinstance(options, options_class):
            driver_options_argument = kwargs.get("driver_options_argument", [])
            driver_options_argument.extend(
                [
                    "--no-sandbox",
                    "--allow-running-insecure-content",
                    "--ignore-certificate-errors",
                    "--start-maximized",
                ]
            )
            for arg in driver_options_argument:
                if not arg.startswith("-"):
                    driver_options_argument.append("--" + str(arg))
            driver_options_argument = list(set(driver_options_argument))

            options = options_class()
            for arg in driver_options_argument:
                options.add_argument(arg)
        options.headless = headless
        # options.add_argument("--disable-extensions")
        # options.add_argument("--disable-infobars")

        return options
