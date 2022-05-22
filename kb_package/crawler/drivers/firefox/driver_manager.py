import re
import sys
import os

from typing import Union

import bs4
import requests
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

from kb_package import tools
from kb_package.crawler.custom_driver import CustomDriver
from kb_package.crawler.drivers.navigators.driver_manager import (
    DriverManager as ParentDriverManager)


class DriverManager(ParentDriverManager):
    NAME = "firefox"
    URL_FOR_WEB_DRIVER_VERSION = "https://firefox-source-docs.mozilla.org/" \
                                 "testing/geckodriver/Support.html"
    URL_FOR_WEB_DRIVER_LIST = "https://github.com/mozilla/geckodriver/" \
                              "releases"
    URL_FOR_EXE = "https://github.com/mozilla/geckodriver/releases/" \
                  f"download/v%s/geckodriver-v%s-%s.%s"
    PATH = os.path.dirname(__file__)
    PATH_TO_REF = os.path.join(PATH, "last_ref.json")

    def __init__(self):
        self.info_platform = tools.get_platform_info()
        exe = "zip"

        if self.info_platform["platform"] != "win":
            exe = "tar.gz"
        if self.info_platform["platform"] == "mac":
            self.info_platform["platform"] = self.info_platform["os"]
            p = self.info_platform["platform"]
            if self.info_platform["bit"] == 64:
                p = self.info_platform["platform"] + "aarch64"
        else:
            p = self.info_platform["platform"] + str(self.info_platform["bit"])

        self.exe = exe
        self.platform = p

    @property
    def find_navigator_version(self):
        platform = sys.platform
        if platform == "linux":
            return os.popen("firefox --version").read().strip().split(" ")[
                -1]

        elif platform == "win32":
            firefox_path = tools.search_file("firefox.exe",
                                             "Mozilla Firefox",
                                             from_path="C:\\",
                                             depth=3)
            assert firefox_path is not None, f"{self.NAME.title()} " \
                                             f"not found"
            cmd = '"' + firefox_path + '" -v|more'

            return os.popen(cmd).read().strip().split(" ")[-1]

        elif platform == "darwin":
            return os.popen(
                'mdls -name kMDItemVersion '
                '/Applications/Firefox.app | tr -d "."').read().strip()

    def download_exe(self, version):
        url = self.URL_FOR_EXE % (version, version, self.platform, self.exe)

        zip_file = os.path.join(self.PATH,
                                "drivers", version,
                                "geckodriver." + self.exe)
        file = os.path.join(os.path.dirname(zip_file),
                            "geckodriver" + self.info_platform['exe'])
        if os.path.exists(file):
            return file
        elif CustomDriver.download_using_link(url, zip_file):
            tools.extract_file(zip_file,
                               to_directory=os.path.dirname(zip_file))
            return file
        return None

    @staticmethod
    def get_lasts_version(page=1):
        versions = []
        while True:
            response = requests.get(
                DriverManager.URL_FOR_WEB_DRIVER_LIST +
                "?page=" + str(page))

            page_object = bs4.BeautifulSoup(response.text, "lxml")
            data = [box.select_one("h1").text.strip().lower()
                    for box in page_object.select(".Box")
                    if box.select_one("h1") is not None
                    ]
            if not len(data):
                break
            versions.extend(data)
            page += 1
        return versions

    def generate_webdriver_exe(self,
                               n_version: Union[int, str] = None):
        if n_version is None:
            n_version = self.find_navigator_version
        assert n_version is not None, "Fail to find CHROME version"
        firefox_version = str(n_version)
        start_v = int(firefox_version.split(".")[0])
        last_ref = tools.CustomFileOpen(self.PATH_TO_REF)
        last_versions = last_ref.data.get(start_v, [])

        for v in last_versions:
            final_file = self.download_exe(v)
            if final_file is not None:
                yield final_file

        response = requests.get(self.URL_FOR_WEB_DRIVER_VERSION)

        page_object = bs4.BeautifulSoup(response.text, "lxml")

        supported_gecko_version = [(
            tr.select_one("td:nth-child(3)"),
            tr.select_one("td:nth-child(4)"),
            tr.select_one("td:nth-child(1)"))
            for tr in page_object.select(
                "#supported-platforms table tr")[2:]
            # if
        ]
        versions = []
        first_v = None
        for v_min, v_max, v in supported_gecko_version:
            if v_min and v_max and v:
                v_min = re.search(r"(\d+)", v_min.text)
                v_max = re.search(r"(\d+)", v_max.text)
                if v_min:
                    v_min = int(v_min.groups()[0])
                else:
                    v_min = -tools.INFINITE
                if v_max:
                    v_max = int(v_max.groups()[0])
                else:
                    v_max = tools.INFINITE
                if first_v is None:
                    first_v = v.text.strip().lower()
                if v_min <= start_v <= v_max:
                    versions.append(v.text.strip().lower())
        last_ref.data[start_v] = versions
        last_ref.save()
        versions = versions + self.get_lasts_version()
        for v in versions:
            final_file = self.download_exe(v)
            if final_file is not None:
                yield final_file

    def extra_args(self, **kwargs):
        extra_args = super().extra_args(**kwargs)
        binary = kwargs.get("binary_location", None)
        if binary:
            extra_args["firefox_binary"] = FirefoxBinary(binary)

        return extra_args

    def get_options(self, **kwargs):
        return self.get_default_options(**kwargs)

    def get_profile(self, **kwargs):
        profile = self.get_default_profile(**kwargs)

        user_agent = kwargs.get("default_ua", None)
        need_authentication = kwargs.get("need_authentication", False)

        use_cache = kwargs.get("use_cache", True)
        use_image = kwargs.get("use_image", False)
        use_js = kwargs.get("use_js", True)
        use_css = kwargs.get("use_css", True)
        use_flash = kwargs.get("use_flash", False)
        download_path = kwargs.get("download_path", os.getcwd())

        disable_pdf_reader = kwargs.get("disable_pdf_reader", False)

        custom_preferences = kwargs.get("custom_preferences", {})

        # cache config setting
        # cache config
        if isinstance(use_cache, dict):
            pass
        else:
            use_cache = {k: bool(use_cache) for k in ["disk", "memory"]}
        for k, v in use_cache.items():
            profile.set_preference(f"browser.cache.{k}.enable", v)
        profile.set_preference("network.http.use-cache", bool(use_cache))

        if need_authentication:
            profile.set_preference("xpinstall.signatures.required", False)
        # images
        profile.set_preference(
            "permissions.default.image", 2 if not use_image else 1
        )
        # Flash
        profile.set_preference(
            "dom.ipc.plugins.enabled.libflashplayer.so",
            "true" if use_flash else "false"
        )
        # css
        profile.set_preference(
            "permissions.default.stylesheet", 2 if not use_css else 1
        )

        # js
        profile.set_preference(
            "javascript.enabled", "true" if use_js else "false"
        )

        # user agent
        if user_agent is not None:
            profile.set_preference(
                "general.useragent.override", user_agent
            )

        for k, v in custom_preferences:
            profile.set_preference(k, v)

        # Auto download of files
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.helperApps.alwaysAsk.force",
                               False)
        profile.set_preference(
            "browser.helperApps.neverAsk.saveToDisk",
            "application/pdf;application/zip;application/x-bzip;"
            "application/octet-stream;multipart/x-zip;"
            "application/zip-compressed;application/x-zip-compressed")
        profile.set_preference(
            "browser.download.manager.showWhenStarting", False)
        # Change default directory for downloads
        profile.set_preference("browser.download.dir", download_path)

        # Don't show PDF in the navigator viewer
        if disable_pdf_reader:
            profile.set_preference(
                "plugin.disable_full_page_plugin_for_types",
                "application/pdf")
            profile.set_preference("pdfjs.disabled", True)

        profile.update_preferences()

        return profile

    def get_desired_capabilities(self, **kwargs):
        return self.get_default_desired_capabilities(**kwargs)
