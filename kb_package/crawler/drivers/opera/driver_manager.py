import os

from typing import Union

import bs4
import requests

from kb_package import tools
from kb_package.crawler.custom_driver import CustomDriver
from kb_package.crawler.drivers.navigators.driver_manager import (
    DriverManager as ParentDriverManager)


class DriverManager(ParentDriverManager):
    NAME = "opera"
    URL_FOR_WEB_DRIVER_VERSION = "https://github.com/operasoftware/" \
                                 "operachromiumdriver/releases"
    URL_FOR_EXE = "https://github.com/operasoftware/operachromiumdriver/" \
                  "releases/download/v.%s/operadriver_%s%s.zip"
    PATH = os.path.dirname(__file__)
    PATH_TO_REF = os.path.join(PATH, "last_ref.json")

    def __init__(self):
        self.info_platform = tools.get_platform_info()

    @property
    def find_navigator_version(self):
        return None

    @staticmethod
    def get_lasts_version(version=None, page=1):
        versions = []
        while True:
            response = requests.get(
                DriverManager.URL_FOR_WEB_DRIVER_VERSION +
                "?page=" + str(page))

            page_object = bs4.BeautifulSoup(response.text, "lxml")
            data = [(box.select_one("h1").text.strip().lower(),
                     box.select_one(".markdown-body a").text.strip().split(
                         " ")[-1]
                     )
                    for box in page_object.select(".Box")
                    if box.select_one("h1") is not None
                    ]
            if version is not None:
                for t, v in data:
                    if v == str(version).strip():
                        return [t]
            if not len(data):
                break
            versions.extend(data)
            page += 1
        return [t for t, v in versions]

    def download_exe(self, version):
        url = self.URL_FOR_EXE % (version, self.info_platform['platform'],
                                  self.info_platform["bit"])

        zip_file = os.path.join(self.PATH,
                                "drivers", version,
                                "operadriver.zip")

        file = os.path.join(os.path.dirname(zip_file),
                            "operadriver" + self.info_platform['exe'])
        if os.path.exists(file):
            return file
        elif CustomDriver.download_using_link(url, zip_file):
            tools.extract_file(zip_file,
                               to_directory=os.path.dirname(zip_file))
            tools.rename_file(
                os.path.join(os.path.dirname(zip_file),
                             "operadriver_" + self.info_platform['platform'] +
                             str(self.info_platform["bit"]),
                             "operadriver" + self.info_platform['exe']
                             ),
                os.path.join(
                    os.path.dirname(zip_file),
                    "operadriver" + self.info_platform['exe']
                ),
                absolute_new_name=True

            )
            return file
        return None

    def generate_webdriver_exe(self,
                               n_version: Union[int, str] = 85):
        if n_version is None:
            n_version = self.find_navigator_version
        assert n_version is not None, "Fail to find CHROME version"
        opera_version = str(n_version)
        start_v = opera_version.split(".")[0]
        last_ref = tools.CustomFileOpen(self.PATH_TO_REF)
        last_versions = last_ref.data.get(start_v, [])

        for v in last_versions:
            final_file = self.download_exe(v)
            if final_file is not None:
                yield final_file

        versions = self.get_lasts_version(version=start_v)

        last_ref.data[start_v] = versions
        last_ref.save()

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
        options.add_experimental_option("prefs", chrome_pref)
        if user_agent is not None:
            options.add_argument(f"user-agent={user_agent}")

        return options


if __name__ == '__main__':
    print(DriverManager().install(85))
