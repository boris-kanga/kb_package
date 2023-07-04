from __future__ import annotations
import re
import shutil
import subprocess
import sys
import os
import io
import tempfile
import traceback
from functools import reduce
import json

from typing import Union
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

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
    CHROME_VERSION = None

    def __init__(self):
        super().__init__()
        self.info_platform = tools.get_platform_info()
        if self.info_platform["os"] != "window":
            self.info_platform["platform"] += "64"
        else:
            self.info_platform["platform"] += "32"

    @property
    def find_navigator_version(self):
        platform = sys.platform
        if self.CHROME_VERSION is not None:
            return self.CHROME_VERSION
        if platform == "linux":
            if self.NAME.lower() == "chrome":
                cmd = "google-chrome --product-version"
                self.CHROME_BINARY = "google-chrome"
            else:
                cmd = "chromium-browser --product-version"
                self.CHROME_BINARY = "chromium-browser"
            self.CHROME_VERSION = os.popen(cmd).read().strip().split(" ")[-1]
            return self.CHROME_VERSION

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
            self.CHROME_VERSION = os.popen(cmd).read().strip()
            return self.CHROME_VERSION
        elif platform == "darwin":
            candidates = (

                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",

            )
            for candidate in candidates:
                if os.path.exists(candidate) and os.access(candidate, os.X_OK):
                    self.CHROME_BINARY = os.path.normpath(candidate)
                    break

            cmd = self.CHROME_BINARY + "--version"
            self.CHROME_VERSION = os.popen(cmd).read().strip().split(" ")[-1]
            return self.CHROME_VERSION

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

    @staticmethod
    def is_binary_patched(executable_path):
        try:
            with io.open(executable_path, "rb") as fh:
                response = fh.read().find(b"undetected chromedriver") != -1
                return response
        except FileNotFoundError:
            return False

    def download_exe(self, version):
        url = self.URL_FOR_EXE % (version, self.info_platform['platform'])

        zip_file = os.path.join(self.PATH,
                                "drivers", version,
                                "chromedriver.zip")
        file = os.path.join(os.path.dirname(zip_file),
                            "chromedriver" + self.info_platform['exe'])
        if os.path.exists(file):
            if not self.is_binary_patched(file):
                self.patch_exe(file)
            return file
        elif CustomDriver.download_using_link(url, zip_file):
            tools.extract_file(zip_file,
                               to_directory=os.path.dirname(zip_file))
            self.patch_exe(file)
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

        for v in versions:
            final_file = self.download_exe(v)
            if final_file is not None:
                self.patch_exe(final_file)
                yield final_file

    @staticmethod
    def patch_exe(exe_file):
        with io.open(exe_file, "r+b") as fh:
            content = fh.read()
            match_injected_codeblock = re.search(rb"\{window\.cdc.*?;\}", content)
            print(match_injected_codeblock)
            if match_injected_codeblock:
                target_bytes = match_injected_codeblock[0]
                new_target_bytes = (
                    b'{console.log("kb_package undetected chromedriver")}'.ljust(
                        len(target_bytes), b" "
                    )
                )
                new_content = content.replace(target_bytes, new_target_bytes)
                fh.seek(0)
                fh.write(new_content)

    def get_options(self, **kwargs):
        options: webdriver.ChromeOptions = self.get_default_options(**kwargs)
        use_flash = kwargs.get("use_flash", False)
        user_agent = kwargs.get("default_ua", None)

        # use_cache = kwargs.get("use_cache", True)
        use_image = kwargs.get("use_image", False)
        use_js = kwargs.get("use_js", True)
        download_folder = kwargs.get("download_folder", os.getcwd())
        os.chmod(download_folder, 0o755)
        disable_pdf_reader = kwargs.get("disable_pdf_reader", False)

        # use_css = kwargs.get("use_css", True)

        custom_preferences = kwargs.get("custom_preferences", {})
        # Change default directory for downloads
        # Auto download of files
        chrome_pref: dict[dict | str | bool] = {}
        chrome_pref.update(options.experimental_options.get("prefs", {
            "download.default_directory": download_folder,
            "disable-popup-blocking": "true",
            "download.prompt_for_download": False,
            "download.directory_upgrade": False
        }))

        if 'binary_location' in kwargs:
            self.CHROME_BINARY = kwargs.get('binary_location')
        if self.CHROME_BINARY is None:
            version = self.find_navigator_version

        # options.set_capability("acceptSslCerts", True)  # ->invalid argument: unrecognized capability: acceptSslCerts

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

        # Don't show PDF in the navigator viewer
        if disable_pdf_reader:
            chrome_pref["plugins.always_open_pdf_externally"] = True

        chrome_pref.update(custom_preferences)
        options.arguments.extend(["--no-default-browser-check", "--no-first-run"])
        options.arguments.extend(["--no-sandbox", "--test-type"])
        options.add_argument("--start-maximized")
        options.add_argument("--no-sandbox")
        options.binary_location = self.CHROME_BINARY
        if kwargs["headless"]:
            if int(str(self.CHROME_VERSION).split(".")[0]) < 108:
                options.add_argument("--headless=chrome")
            else:
                options.add_argument("--headless=new")

        options.add_experimental_option("prefs", chrome_pref)
        # options.add_experimental_option("useAutomationExtension", False)
        """
        if not use_flash:
            options.set_capability("chromeOptions", {
                "args": ["--disable-plugins"]
            })
        """

        # remote_debugging
        remote_debugging = options.debugger_address or kwargs.get("remote_debugging")
        remote_debugging, port = remote_debugging.split(":")
        port = int(port)
        """
        map_coord = {
            "latitude": 42.1408845,
            "longitude": -72.5033907,
            "accuracy": 100
        }
        web_driver.execute_cdp_cmd("Emulation.setGeolocationOverride", map_coord)
        
        """

        if not (remote_debugging.endswith(("localhost", "127.0.0.1")) and port == 0):
            options.add_argument("--remote-debugging-host=%s" % remote_debugging)

            options.add_argument("--remote-debugging-port=%s" % port)
            options.debugger_address = "%s:%d" % (remote_debugging, port)

            user_data_dir = kwargs.get("user_data_dir", os.path.normpath(tempfile.mkdtemp()))

            arg = "--user-data-dir=%s" % user_data_dir
            options.add_argument(arg)
            if hasattr(options, "handle_prefs"):
                options.handle_prefs(user_data_dir)

            def parse_prefs(key, value):
                if "." in key:
                    key, rest = key.split(".", 1)
                    value = parse_prefs(rest, value)
                return {key: value}

            parsing_prefs = reduce(
                lambda d1, d2: {**d1, **d2},
                (parse_prefs(key, value) for key, value in chrome_pref.items()),
            )
            try:
                # create the preferences json file in its default directory
                default_dir = os.path.join(user_data_dir, "Default")
                print(default_dir)

                os.makedirs(default_dir, exist_ok=True)

                with open(os.path.join(default_dir, "Preferences"), encoding="latin1", mode="w+") as f:
                    try:
                        prefs = json.load(f)
                        if prefs["profile"]["exit_type"] is not None:
                            # fixing the restore-tabs-nag
                            prefs["profile"]["exit_type"] = None
                    except (json.JSONDecodeError, KeyError, Exception):
                        prefs = {}

                    prefs.update(parsing_prefs)
                    f.seek(0, 0)
                    json.dump(prefs, f)
            except:
                traceback.print_exc()

            del options._experimental_options["prefs"]
        if user_agent is not None:
            options.add_argument(f"user-agent={user_agent}")

        return options


class ChromeProfile:
    DEFAULT_CHROME_FOLDER = os.path.join(os.path.dirname(__file__), "default_options.zip")

    def __init__(self, path=None, chrome_bin=None):
        if path is None:
            path = os.path.normpath(tempfile.mkdtemp())
        self._path = path
        chrome_manager = DriverManager()

        if chrome_bin is None:
            version = chrome_manager.find_navigator_version
            chrome_bin = chrome_manager.CHROME_BINARY
        else:
            chrome_manager.CHROME_BINARY = chrome_bin
            version = chrome_manager.find_navigator_version
        self._bin = chrome_bin
        if not os.path.exists(self.DEFAULT_CHROME_FOLDER):

            proc = subprocess.Popen(
                [self._bin, "--user-data-dir=%s" % os.path.splitext(self.DEFAULT_CHROME_FOLDER)[0], "--headless=new"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            default_pref = os.path.join(os.path.splitext(self.DEFAULT_CHROME_FOLDER)[0], "Default", "Preferences")
            while True:
                try:
                    options = webdriver.ChromeOptions()
                    options.add_argument("--headless=new")
                    for executable in chrome_manager.generate_webdriver_exe(version):
                        d = webdriver.Chrome(service=Service(executable_path=executable), options=options)
                        d.get("https://google.com")
                        break

                    assert tools.read_json_file(default_pref, 1) != 1
                    proc.kill()
                    break
                except AssertionError:
                    pass
            shutil.make_archive(os.path.splitext(self.DEFAULT_CHROME_FOLDER)[0],
                                "zip",
                                root_dir=os.path.dirname(__file__))
            # shutil.rmtree(os.path.splitext(self.DEFAULT_CHROME_FOLDER)[0], ignore_errors=True)

    def install(self):

        pass

    @property
    def path(self):
        return self._path


if __name__ == '__main__':
    d = ChromeProfile()
