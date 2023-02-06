import re
import sys
import os

from typing import Union

import bs4
import requests
from kb_package import tools
from kb_package.crawler.custom_driver import CustomDriver
from kb_package.crawler.drivers.navigators.driver_manager import (
    DriverManager as ParentDriverManager)


class DriverManager(ParentDriverManager):
    NAME = "chrome"
    TRUTH_NAME = "Chrome"
    URL_FOR_WEB_DRIVER_VERSION = "https://chromedriver.chromium.org/downloads"
    URL_FOR_EXE = "https://chromedriver.storage.googleapis.com/" \
                  "%s/chromedriver_%s.zip"
    PATH = os.path.dirname(__file__)
    PATH_TO_REF = os.path.join(PATH, "last_ref.json")

    CHROME_BINARY = None

    def __init__(self):
        self.info_platform = tools.get_platform_info()
        if self.info_platform["os"] != "window":
            self.info_platform["platform"] += "64"
        else:
            self.info_platform["platform"] += "32"

    @property
    def find_navigator_version(self):
        platform = sys.platform
        if platform == "linux":
            if self.NAME.lower() == "chrome":
                cmd = "google-chrome --product-version"
            else:
                cmd = "chromium-browser --product-version"
            return os.popen(cmd).read().strip().split(" ")[-1]

        elif platform == "win32":
            if self.CHROME_BINARY is None:
                chrome_path = tools.search_file("chrome.exe", "Application",
                                                from_path="C:\\",
                                                depth=4)
                self.CHROME_BINARY = chrome_path
            else:
                chrome_path = self.CHROME_BINARY
            assert chrome_path is not None, f"{self.TRUTH_NAME.title()} " \
                                            f"not found"
            cmd = "powershell -command \"&{(Get-Item '%s')." \
                  "VersionInfo.ProductVersion}\"" % chrome_path

            return os.popen(cmd).read().strip()
        elif platform == "darwin":
            # for mac
            # check for chromium
            cmd = "/Applications/Google\ Chrome.app/Contents/MacOS/Google\ " \
                  "Chrome --version"
            return os.popen(cmd).read().strip().split(" ")[-1]

    @staticmethod
    def get_lasts_version():
        response = requests.get(DriverManager.URL_FOR_WEB_DRIVER_VERSION)

        page_object = bs4.BeautifulSoup(response.text, "html.parser")
        return [re.match(fr"chromedriver\s([\d.]+)",
                         a.text.strip().lower(),
                         flags=re.I).groups()[0]
                for a in page_object.select("a.XqQF9c:not(.rXJpyf)")
                if re.match(fr"chromedriver\s[\d.]+",
                            a.text.strip().lower(), flags=re.I)]

    def download_exe(self, version):
        url = self.URL_FOR_EXE % (version, self.info_platform['platform'])

        zip_file = os.path.join(self.PATH,
                                "drivers", version,
                                "chromedriver.zip")
        file = os.path.join(os.path.dirname(zip_file),
                            "chromedriver" + self.info_platform['exe'])
        if os.path.exists(file):
            return file
        elif CustomDriver.download_using_link(url, zip_file):
            tools.extract_file(zip_file,
                               to_directory=os.path.dirname(zip_file))
            return file
        return None

    def generate_webdriver_exe(self,
                               n_version: Union[int, str] = None):
        if n_version is None:
            n_version = self.find_navigator_version
        assert n_version is not None, "Fail to find CHROME version"
        chrome_version = str(n_version)
        start_v = chrome_version.split(".")[0]
        last_ref = tools.CustomFileOpen(self.PATH_TO_REF)
        last_versions = last_ref.data.get(start_v, [])
        print(last_versions)
        for v in last_versions:
            final_file = self.download_exe(v)
            if final_file is not None:
                yield final_file

        response = requests.get(self.URL_FOR_WEB_DRIVER_VERSION)

        page_object = bs4.BeautifulSoup(response.text, "html.parser")
        versions = [re.match(r"chromedriver\s([\d.]+)",
                             a.text.strip().lower(),
                             flags=re.I).groups()[0]
                    for a in page_object.select("a.XqQF9c:not(.rXJpyf)")
                    if re.match(r"chromedriver\s([\d.]+)",
                                a.text.strip().lower(),
                                flags=re.I)]
        _versions = [{
            "dx": abs(int(v.split(".")[0].strip()) - int(start_v)),
            "v": v
        } for v in versions]
        versions = [v["v"] for v in _versions if v["dx"] == min([v["dx"] for v in _versions])]
        last_ref.data[start_v] = list(set(versions))
        last_ref.save()

        print(versions)

        for v in versions:
            final_file = self.download_exe(v)
            if final_file is not None:
                yield final_file

    def get_desired_capabilities(self, **kwargs):
        use_flash = kwargs.get("use_flash", False)
        desired_capabilities = self.get_default_desired_capabilities(**kwargs)
        if not use_flash:
            desired_capabilities["chromeOptions"] = {
                "args": ["--disable-plugins"]
            }
        return desired_capabilities

    def get_profile(self, **kwargs):
        return self.get_default_profile(**kwargs)

    def get_options(self, **kwargs):
        options = self.get_default_options(**kwargs)
        user_agent = kwargs.get("default_ua", None)

        # use_cache = kwargs.get("use_cache", True)
        use_image = kwargs.get("use_image", False)
        use_js = kwargs.get("use_js", True)
        download_path = kwargs.get("download_path", os.getcwd())
        disable_pdf_reader = kwargs.get("disable_pdf_reader", False)

        # use_css = kwargs.get("use_css", True)

        custom_preferences = kwargs.get("custom_preferences", {})

        chrome_pref = {}

        if 'binary_location' in kwargs:
            self.CHROME_BINARY = kwargs.get('binary_location')

        # cache config setting
        # images
        if not use_image:
            chrome_pref["profile.default_content_setting_values"] = {
                "images": 2
            }
        # css
        # js
        if not use_js:
            if "profile.default_content_setting_values" not in chrome_pref:
                chrome_pref["profile.default_content_setting_values"] = {}
            chrome_pref["profile.default_content_setting_values"][
                "javascript"
            ] = 2
        # Change default directory for downloads
        chrome_pref["download.default_directory"] = download_path
        # Auto download of files
        chrome_pref["download.prompt_for_download"] = False
        chrome_pref["download.directory_upgrade"] = False

        # Don't show PDF in the navigator viewer
        if disable_pdf_reader:
            chrome_pref["plugins.always_open_pdf_externally"] = True

        chrome_pref.update(custom_preferences)
        options.add_argument("--no-sandbox")
        options.add_experimental_option("prefs", chrome_pref)
        options.add_experimental_option("useAutomationExtension", False)
        if user_agent is not None:
            options.add_argument(f"user-agent={user_agent}")

        return options
