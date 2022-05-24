# -*- coding: utf-8 -*-
"""
The CustomDriver helper.
Use to create selenium web driver with a specific configuration
"""
import json
import random
import shutil
import time
import importlib
import os
import traceback

from typing import Union

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import requests

from kb_package import tools
from kb_package.custom_proxy.custom_proxy import CustomProxy

try:
    from kb_package.logger.customlogger import CustomLogger
except ImportError:
    import logging

    CustomLogger = logging.getLogger


class CustomDriver:
    CHROME = 1
    FIREFOX = 2
    OPERA = 3
    SET_OF_NAVIGATOR = {CHROME: "chrome", FIREFOX: "firefox", OPERA: "opera"}
    SUPPORT_URL = "about:support"

    PATH = os.path.dirname(__file__)
    PLUGIN_DIR = os.path.join(PATH,
                              "plugin-authentication")

    URL_FOR_CHROME_WEB_DRIVER = "https://chromedriver.chromium.org/downloads"
    URL_FOR_FIREFOX_WEB_DRIVER = "https://github.com/mozilla/geckodriver/" \
                                 "releases"
    DEFAULT_UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36' \
                 ' (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'

    def __init__(self, *args, **kwargs):
        self._driver = None
        self._args = args
        self._kwargs = kwargs
        self.origin_tab = None

    def create(self, *args, **kwargs):
        try:
            self._driver.close()
        except AttributeError:
            pass
        self._driver = CustomDriver.create_driver(
            *(self._args or args), **(self._kwargs or kwargs))
        self.origin_tab = self._driver.window_handles[0]

    def __call__(self, *args, **kwargs):
        return self._driver

    @staticmethod
    def download_using_link(link, file_name):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        try:
            response = requests.get(link, stream=True)
            with open(file_name, "wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)
            return True

        except Exception:
            return False

    @staticmethod
    def create_user_agent():
        """
        Get random user agent
        Returns: dict, like {"chrome":[UA], "firefox":[UA]}
        """
        try:
            return UserAgent().data_browsers
        except (IndexError, Exception):
            return {
                k: [
                    CustomDriver.DEFAULT_UA
                ]
                for k in CustomDriver.SET_OF_NAVIGATOR.values()
            }

    @staticmethod
    def screenshot_all_page(driver, scroll_what=None, bottom_selector=None,
                            dir_=os.getcwd()):
        screenshots_files_list = []
        i = 1
        path = os.path.join(dir_, f"screenshot_{i}.png")
        screenshots_files_list.append(path)
        driver.save_screenshot(path)
        response = False
        try:
            while not response:
                i += 1
                response, dist = driver.execute_script(
                    '''
                    function kb_scroll(what=null, to=null){
                        what = document.querySelector(what) || window;
                        let last_scrollY = what.scrollY || what.scrollTop || 0;
                        let height = what.offsetHeight || what.innerHeight
                        what.scrollBy(0, height-0.1 * height);
                        let elem=document.querySelector(to);
                        let new_scrollY = what.scrollY || what.scrollTop || 0;
                        let max_y = height;
                        let bool_elem = false;
                        
                        if (elem){
                            let top = elem.getBoundingClientRect().top;
                            let elem_size = top + elem.offsetHeight;
                            bool_elem = (max_y >= elem_size);
                        }
                        return [(bool_elem ||
                                (last_scrollY>=new_scrollY - 1)), 
                                new_scrollY- last_scrollY - 1];
                    }
                    return kb_scroll(arguments[0], arguments[1]);
                    ''', scroll_what, bottom_selector)
                if dist > 0:
                    path = os.path.join(dir_, f"screenshot_{i}.png")
                    screenshots_files_list.append(path)
                    driver.save_screenshot(path)
                    time.sleep(0.2)
        except:
            pass
        return screenshots_files_list

    @staticmethod
    def open_new_tab(driver, for_url="", switch_on=True, load_page=None):
        load_page = (for_url.strip() not in ["", "#"] if load_page is None
                     else load_page)
        last_tabs_open = driver.window_handles

        wait = WebDriverWait(driver, 10)
        if driver.desired_capabilities["browserName"] == \
                CustomDriver.SET_OF_NAVIGATOR[CustomDriver.FIREFOX]:
            driver.execute_script(f"""
                if (document.querySelector(
                        "#kbp-open-link-identifier")!=null) return;
                let a=document.createElement("a"); 
                a.id="kbp-open-link-identifier";
                document.querySelector("body").appendChild(a);
                a.href='{for_url}';
            """)
            driver.find_element_by_css_selector(
                "#kbp-open-link-identifier").send_keys(
                Keys.CONTROL + Keys.RETURN)
        else:
            driver.execute_script(f"window.open('{for_url}');")
        wait.until(EC.number_of_windows_to_be(len(last_tabs_open) + 1))
        # self.driver.set_page_load_timeout(timeout_infos["page_load_timeout"])
        new_tab_id = [tab for tab in driver.window_handles if
                      tab not in last_tabs_open][0]

        if switch_on:
            driver.switch_to.window(new_tab_id)
            if not load_page:
                driver.execute_script("window.stop();")
        return new_tab_id

    def switch_to_parent_driver(self):
        try:
            self._driver.switch_to.window(self.origin_tab)
        except:
            self.origin_tab = self._driver.window_handles[0]
            self._driver.switch_to.window(self.origin_tab)

    @staticmethod
    def _get_driver(
            options,
            desired_capabilities,
            profile=None,
            executable_path=None,
            logger=CustomLogger("CustomDriver"),
            extensions: list = None,
            max_try=3,
            sleep_time=5,
            maximize=True,
            extra_args=None
    ):
        """
        Use to create web driver using options, desired_capabilities and/or
        profile
        Args:
            options: webDriver.Options
            desired_capabilities: webDriver.DesiredCapabilities
            profile: webDriver.FirefoxProfile or None
            executable_path: str, path webDriver exe
            logger: logger
            extensions: list of dict {name, path}
            max_try: int max try when got fail
            sleep_time: sleep time between two creation fails
            maximize: bool, for driver window maximize size

        Returns: web driver

        """
        assert desired_capabilities["browserName"] \
               in CustomDriver.SET_OF_NAVIGATOR.values()
        driver = None
        if not isinstance(extra_args, dict):
            extra_args = {}
        for _ in range(max_try):
            try:
                if (
                        desired_capabilities["browserName"]
                        == CustomDriver.SET_OF_NAVIGATOR[CustomDriver.FIREFOX]
                ):
                    driver = webdriver.Firefox(
                        options=options,
                        desired_capabilities=desired_capabilities,
                        firefox_profile=profile,
                        executable_path=executable_path,
                        **extra_args
                    )

                    for extension in (extensions or []):
                        driver.install_addon(extension["path"],
                                             temporary=True)
                    # check if extensions is really installed
                    driver.get("about:support")
                    addons = driver.execute_script("""
                        return [...document.querySelectorAll(
                        '#addons-tbody tr')].map(tr=>{
                            return {name:tr.cells[0].innerText}
                        });""")
                    extensions_really_installed = [
                        on["name"].lower() for on in addons
                        if str(on["name"]).lower() in [
                            e["name"].lower() for e in (extensions or [])
                        ]
                    ]

                    error_extension = [
                        e for e in [
                            e["name"].lower() for e in (extensions or [])
                        ] if e not in extensions_really_installed
                    ]
                    if error_extension:
                        error_extension = "Fail to install extensions" \
                                          ": %s" % error_extension
                    else:
                        error_extension = ""
                    assert len(error_extension) == 0, error_extension
                else:
                    for extension in (extensions or []):
                        options.add_extension(extension["path"])
                    driver = webdriver.Chrome(
                        options=options,
                        desired_capabilities=desired_capabilities,
                        executable_path=executable_path,
                        **extra_args
                    )

                if maximize:
                    driver.maximize_window()
                return driver

            except Exception as e:
                logger.exception(e)
                logger.info("Driver creation failed, retry in %s", sleep_time)
                time.sleep(sleep_time)
                try:
                    driver.close()
                except AttributeError:
                    pass
        raise Exception("Fail to create the driver")

    @staticmethod
    def create_driver(
            navigator: Union[int, str] = 2,
            logger=CustomLogger("CustomDriver"), **kwargs
    ):
        """
        Use to create new web driver using a specific config

        Args:
            navigator: int, value includes in the class attribute
                SET_OF_NAVIGATOR
            logger: the logger
            **kwargs:
                executable_path: str, which specify path of web driver
                    executable, default None

                n_version: (str, int), the navigator version. Useful where
                    auto_exe_path is True for download the right webdriver

                auto_exe_path: bool, auto download the right webdriver for
                    selenium. Default False

                proxy_val: Union[str, dict, int],
                    -For custom_proxy which don't need authentication
                                don't mention user or password arg
                        Examples: "127.0.0.1:24000" or "localhost:24000" or
                            24000 or {"host":, "port":}
                    -For custom_proxy which require authentication:
                            Examples: "user:pwd@host:port" or
                                {"host":, "pwd|password":, "user":, "port":}

                    default None (No custom_proxy use}

                headless: bool, For navigation headless, default False
                user_agent: str, default None
                random_user_agent: bool, if True, generate random user agent,
                    default True
                use_cache: bool|dict, default True
                use_image: bool, default False mean images are disable for the
                    driver
                use_js: bool, default True mean javascript is enable
                use_css: bool, default True mean css is enable
                use_flash: bool, default True mean flash is enable. This option
                    is only consider for firefox driver
                binary_location: path, location of the navigator apps,
                    Default None,
                driver_options_argument: list, list of argument to add to
                    options argument. Default None
                custom_preferences: dict, like {"key":value}. Default None,
                    additional preference to add when
                    creating the driver
                driver_options: webdriver.Options instance, default None
                desired_capabilities: webdriver.DesiredCapabilities object,
                    default None
        Returns: WebDriver object

        """
        error_navigator = (
            "Your selection of navigator must be int or string value like: "
            "([1 | CHROME], [2 | FIREFOX])"
        )
        assert isinstance(navigator, (int, str)), error_navigator
        if isinstance(navigator, str):
            assert navigator.lower() in \
                   CustomDriver.SET_OF_NAVIGATOR.values(), error_navigator
            navigator_str = navigator.lower()
            navigator = {v: k for k, v
                         in CustomDriver.SET_OF_NAVIGATOR.items()}[
                navigator_str]
        else:
            assert navigator in CustomDriver.SET_OF_NAVIGATOR, error_navigator
            navigator_str = CustomDriver.SET_OF_NAVIGATOR[navigator]
        default_executable_path = None
        plugin_file = str(os.getpid()) + "-plugin."
        plugin_extension = "xpi"
        if navigator == CustomDriver.CHROME:
            default_executable_path = "chromedriver"
            plugin_extension = "crx"
        elif navigator == CustomDriver.FIREFOX:
            default_executable_path = "geckodriver"
        plugin_file += plugin_extension
        auto_exe_path = kwargs.get("auto_exe_path", False)
        n_version = kwargs.get("n_version", None)
        executable_path = kwargs.get(
            "executable_path", default_executable_path if
            not auto_exe_path else None
        )

        proxy_val = kwargs.get("proxy_val", None)
        custom_proxy = CustomProxy(proxy_val)
        random_user_agent = kwargs.get(
            "random_user_agent",
            True if custom_proxy else False)
        ua = None
        if random_user_agent:
            ua = random.choice(CustomDriver.create_user_agent()[navigator_str])
        user_agent = kwargs.get("default_ua", ua)

        kwargs["default_ua"] = user_agent

        extensions = None

        kwargs["need_authentication"] = custom_proxy.need_authentication

        # package: kb_package.crawler.drivers.navigators.driver_manager
        driver_manager = getattr(importlib.import_module(
            "kb_package.crawler.drivers.%s.driver_manager"
            % navigator_str),
            "DriverManager")()

        options = driver_manager.get_options(**kwargs)
        profile = driver_manager.get_profile(**kwargs)
        desired_capabilities = driver_manager.get_desired_capabilities(
            **kwargs)
        extra_args = driver_manager.extra_args(**kwargs)

        if custom_proxy:

            driver_proxy = Proxy()
            driver_proxy.proxy_type = ProxyType.MANUAL

            if custom_proxy.need_authentication:
                plugin_path = CustomDriver.PLUGIN_DIR
                plugin_file = os.path.join(plugin_path, plugin_file)
                manifest_file = os.path.join(plugin_path, "manifest.json")
                background_file = os.path.join(plugin_path, "background.js")

                import zipfile
                manifest_file = tools.read_json_file(manifest_file)
                manifest_name = manifest_file["name"]
                with open(background_file) as background:
                    background_js = background.read() % custom_proxy.get()
                with zipfile.ZipFile(plugin_file, 'w') as zp:
                    zp.writestr("manifest.json", json.dumps(manifest_file))
                    zp.writestr("background.js", background_js)
                plugin_file = os.path.realpath(plugin_file)
                extensions = [{"name": manifest_name, "path": plugin_file}]
            else:

                for k in ["http_proxy", "sslProxy"]:
                    setattr(driver_proxy, k, str(custom_proxy))
            driver_proxy.add_to_capabilities(desired_capabilities)

        if auto_exe_path and executable_path is None:

            for executable_path in driver_manager.generate_webdriver_exe(
                    n_version=n_version):
                try:
                    os.chmod(executable_path, 0o755)
                except (PermissionError, Exception):
                    pass
                try:
                    return CustomDriver._get_driver(
                        options=options,
                        desired_capabilities=desired_capabilities,
                        profile=profile,
                        executable_path=executable_path,
                        logger=logger,
                        extensions=extensions,
                        max_try=1,
                        sleep_time=0,
                        extra_args=extra_args
                    )
                except:
                    pass
            raise Exception("Driver creation fail")

        else:
            return CustomDriver._get_driver(
                options=options,
                desired_capabilities=desired_capabilities,
                profile=profile,
                executable_path=executable_path,
                logger=logger,
                extensions=extensions,
                extra_args=extra_args
            )


if __name__ == "__main__":

    from selenium.webdriver.common.by import By
    USER_ID = "id"
    USER_PWD = "pass"
    URL = "https://lite-1x4635600.top/fr/othergames?product=849"
    URL_auth = "https://lite-1x4635600.top/fr/othergames"
    FILE = "coef.txt"

    driver = CustomDriver.create_driver(navigator="firefox",
                                        auto_exe_path=True,
                                        use_image=True)
    wait = WebDriverWait(driver, 5 * 60)
    while True:
        try:
            driver.get(URL_auth)
            time.sleep(5)
            print("Url got")
            driver.execute_script("""
                    document.querySelector(
                        ".user-control-panel__group--auth button").click()
                    """)
            print("click on connection button")
            break
        except:
            print("Une erreur s'est produite")
            time.sleep(1)

    wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, ".auth-form-fields")))
    time.sleep(5)
    print("auth form is now open")
    user = driver.find_element_by_css_selector(
        ".auth-form-fields input.input-field")
    user.send_keys(USER_ID)
    print("user is send")
    time.sleep(5)

    pass_ = driver.find_element_by_css_selector(
        ".auth-main__form div:nth-child(3) input")
    pass_.send_keys(USER_PWD)
    print("pass is send")
    time.sleep(5)

    driver.execute_script("""
    document.querySelector(
        ".auth-form-fields button[type=submit]").click();
    """)
    print("auth form is now submitted")

    time.sleep(20)
    while True:
        try:
            with open(FILE, "w") as file:
                file.write("")
            driver.get(URL)
            print("URL got")
            iframe = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#game_place_game")))
            driver.switch_to.frame(iframe)
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".payouts-block")))
            print("Le jeu s'est affiché")

            last_value = driver.execute_script("""
                            return document.querySelector(".payouts-block").innerText
                        """)
            last_value = last_value.split("\n")
            last_value = [float(x.split("x")[0]) for x in last_value[::-1]]
            print("got last values", last_value)
            with open(FILE, "w") as file:
                file.writelines([str(x)+"\n" for x in last_value])
            nb_of_new = None
            iter_index = 0
            while True:
                not_present = None
                while not_present is None:
                    not_present = driver.execute_script("""
                    let d = document.querySelector(".dom-container .flew-coefficient");
                    return d==null? null: d.innerText""")
                    if not_present is None:
                        time.sleep(1)
                print("got new value for index", iter_index, ":", not_present)
                try:
                    not_present = float(not_present.split("x")[0])
                except:
                    not_present = 0
                last_value.append(not_present)
                print("En attente de la prochaine partie")
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".dom-container .bet-timer")))
                print("Lancement du nouveau jeu ...")
                if isinstance(nb_of_new, int) and iter_index > nb_of_new:
                    break
                iter_index += 1
                with open(FILE, "a") as file:
                    file.write(str(not_present)+"\n")
        except KeyboardInterrupt:
            print("Aurevoir")
            driver.close()
            break
        except:
            traceback.print_exc()
            print("Une erreur s'est produite, On réessaie dans 10 secondes")
            time.sleep(10)
