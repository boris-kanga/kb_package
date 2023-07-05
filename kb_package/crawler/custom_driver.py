# -*- coding: utf-8 -*-
"""
The CustomDriver helper.
Use to create selenium web driver with a specific configuration
"""
import atexit
import random
import re
import shutil
import subprocess
import time
import importlib
import os
import traceback
import typing
from contextlib import contextmanager
import signal


try:
    from fake_useragent import UserAgent
except (ImportError, Exception):
    class UserAgent:
        pass
from selenium import webdriver
from selenium.webdriver.common.by import By as SelectBy
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import requests
from kb_package import tools
from kb_package.crawler.navigation_errors.error_handler import (
    ErrorHandler
)
from kb_package.custom_proxy.custom_proxy import CustomProxy

try:
    from kb_package.logger.customlogger import CustomLogger
except ImportError:
    import logging

    CustomLogger = logging.getLogger

REGISTERED = []


def _get_default_navigator():
    folder = os.path.join(os.path.dirname(__file__), "drivers")
    supported_nav = os.listdir(folder)
    for nav in supported_nav:
        if not os.path.isdir(os.path.join(folder, nav)) or nav.startswith("__"):
            continue
        try:
            # print(nav)
            driver_manager = getattr(importlib.import_module(
                "kb_package.crawler.drivers.%s.driver_manager"
                % nav),
                "DriverManager")()
            assert len(str(driver_manager.find_navigator_version).strip())
            return nav
        except (ImportError, AttributeError, Exception):
            traceback.print_exc()
            pass


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
    _USE_FAKE_USER_AGENT = False

    def __init__(self, *args, **kwargs):
        self._driver = None
        self._args = args
        self._kwargs = kwargs
        self.origin_tab = None
        self.logger = kwargs.get("logger") or CustomLogger("CustomDriver")

    @property
    def get_download_folder(self):
        return self._kwargs.get("download_folder", os.getcwd())

    def create(self, *args, **kwargs):
        if self._driver is not None:
            return
        self._driver = CustomDriver.create_driver(
            *(self._args or args), **(self._kwargs or kwargs))
        self.origin_tab = self._driver.window_handles[0]

    def __call__(self, *args, **kwargs):
        if self._driver is None:
            self.create(*args, **kwargs)
        return self._driver

    def stop(self):
        try:
            self._driver.quit()
        except (AttributeError, Exception):
            pass
        finally:
            self._driver = None

    def kill(self):
        self.stop()

    def quit(self):
        self.stop()

    def close(self):
        self.stop()

    def __getattr__(self, item, *args):
        return getattr(self._driver, item)

    def __enter__(self):
        if self._driver is None:
            self.create()
        return self._driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._driver.close()
        except(AttributeError, Exception):
            pass
        self._driver = None

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
        default = {
                k: [
                    CustomDriver.DEFAULT_UA
                ]
                for k in CustomDriver.SET_OF_NAVIGATOR.keys()
            }
        if not CustomDriver._USE_FAKE_USER_AGENT:
            return default
        try:
            ref = {v: k for k, v in CustomDriver.SET_OF_NAVIGATOR.items()}
            return {ref[k]: v for k, v in UserAgent().data_browsers.items()}
        except (IndexError, AttributeError, Exception):
            return default

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
                    # time.sleep(0.2)
        except:
            pass
        return screenshots_files_list

    def js(self, script, *args, **kwargs):
        return self._driver.execute_script(script, *args, **kwargs)

    def sleep_until_load(
            self, max_try=3,
            css_selector="img[src], a[href], h1,h2,h3,h4,h5,h6",
            check_error=True, get_data=False, apply=None, match=None
    ):
        """
        Wait for dom load
        Args:
            max_try: int, time in second for wait for dom loaded
            css_selector: str, criteria for dom loaded judgment
            get_data: bool, get the value returns by css_selector
            check_error: bool, if it necessary to check any error in the dom
            apply: function to apply of result for using @match
            match: list or reg

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
                    if match is not None:
                        if callable(apply):
                            data = apply(data)
                        if isinstance(match, str):
                            match = [match]
                        if isinstance(match, (list, tuple)):
                            for d in match:
                                if re.match(d, str(data), flags=re.I | re.S):
                                    got = True
                                    break
                            if not got:
                                data = None
                                error = "Finished to load tag %s not found" % (str(match))

                        else:
                            got = False
                            error = "Finished to load tag %s not found" % (str(match))
                    else:
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

    @staticmethod
    def open_new_tab(driver, for_url="", switch_on=True, load_page=None):
        load_page = (for_url.strip() not in ["", "#"] if load_page is None
                     else load_page)
        last_tabs_open = driver.window_handles

        wait = WebDriverWait(driver, 10)

        driver.execute_script(f"""
            if (document.querySelector(
                    "#kbp-open-link-identifier")!=null) return;
            let a=document.createElement("a"); 
            a.id="kbp-open-link-identifier";
            a.style=`width:100%!important;height:100%!important;
                position:absolute!important;top:0!important;left:0!important;
                background:#eeee;
                display:block!important`;
            document.querySelector("body").appendChild(a);
            a.href='{for_url}';
        """)
        link = driver.find_element(by=SelectBy.ID, value="kbp-open-link-identifier")
        link.send_keys(Keys.CONTROL + Keys.RETURN)
        driver.execute_script('document.querySelector("#kbp-open-link-identifier").style.display="none";')

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

    @contextmanager
    def loading_action(self, time_out=tools.INFINITE, verbose=False):
        self._driver.execute_script("window.$KB_LOADING_VARIABLE = 1")
        start_time = time.time()
        yield self._driver
        loader = tools.ConsoleFormat.processing()
        while self._driver.execute_script("return window.$KB_LOADING_VARIABLE") and (time.time()-start_time) < time_out:
            if verbose:
                next(loader)
                time.sleep(0.01)
        if verbose:
            print("Page is now loading")

    @staticmethod
    def _get_driver(
            options,
            executable_path=None,
            logger=CustomLogger("CustomDriver"),
            extensions: list = None,
            keep_alive=True,
            max_try=1,
            sleep_time=5,
            maximize=True,
            **kwargs
    ):
        """
        Use to create web driver using options
        profile
        Args:
            options: webDriver.Options
            profile: webDriver.FirefoxProfile or None
            executable_path: str, path webDriver exe
            logger: logger
            extensions: list of dict {name, path}
            max_try: int max try when got fail
            sleep_time: sleep time between two creation fails
            maximize: bool, for driver window maximize size

        Returns: web driver

        """
        desired_capabilities = options.to_capabilities()
        nav = desired_capabilities["browserName"].lower()
        service = getattr(importlib.import_module("selenium.webdriver.%s.service" % nav), "Service")(
            executable_path=executable_path, **kwargs)

        assert nav in CustomDriver.SET_OF_NAVIGATOR.values()
        driver = None
        for _ in range(max_try):
            try:
                if nav == CustomDriver.SET_OF_NAVIGATOR[CustomDriver.FIREFOX]:
                    driver = webdriver.Firefox(
                        service=service,
                        options=options,
                        keep_alive=keep_alive
                    )
                    for extension in (extensions or []):
                        driver.install_addon(extension["path"],
                                             temporary=True)

                    # check if extensions is really installed
                    driver.get("about:support")
                    wait = WebDriverWait(driver, 10)
                    wait.until(EC.presence_of_element_located((SelectBy.ID, "addons-tbody")))
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
                        service=service,
                        options=options,
                        keep_alive=keep_alive
                    )
                    if options.headless:
                        CustomDriver._configure_headless(driver, logger.info)
                try:
                    if maximize:
                        driver.maximize_window()
                except:
                    pass
                return driver

            except Exception as e:
                logger.exception(e)
                if sleep_time > 0:
                    logger.info("Driver creation failed, retry in %s", sleep_time)
                    time.sleep(sleep_time)
                try:
                    driver.close()
                except AttributeError:
                    pass
        raise Exception("Fail to create the driver")

    @staticmethod
    def create_driver(
            navigator: typing.Optional[typing.Union[str, int]] = None,
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
                remote_debugging: str(host:port) default None,
                auto_remote_debug bool, Default False
                port: int , is about remote_debugging port
                user_agent: str, default None
                random_user_agent: bool, if True, generate random user agent,
                    default True
                use_cache: bool|dict, default True
                use_image: bool, default False mean images are disable for the
                    driver
                use_js: bool, default True mean javascript is enabled
                use_css: bool, default True mean css is enabled
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
        if navigator is None:
            navigator = _get_default_navigator()
        assert isinstance(navigator, (int, str)), error_navigator
        if isinstance(navigator, str):
            if navigator.lower() == "chromium":
                navigator = 1
                navigator_str = "chromium"
            else:
                assert navigator.lower() in \
                       CustomDriver.SET_OF_NAVIGATOR.values() or navigator.lower() == "chromium", error_navigator
                navigator_str = navigator.lower()
                navigator = {v: k for k, v
                             in CustomDriver.SET_OF_NAVIGATOR.items()}[
                    navigator_str]
        else:
            assert navigator in CustomDriver.SET_OF_NAVIGATOR, error_navigator
            navigator_str = CustomDriver.SET_OF_NAVIGATOR[navigator]
        kwargs["headless"] = kwargs.get("headless", bool(os.environ.get("CUSTOM-DRIVER-HEADLESS")))

        # remote debugging
        port = int(kwargs.get("port") or 0)
        remote_debugging = kwargs.get("remote_debugging", "127.0.0.1:%d" % port)
        remote_debugging, port = remote_debugging.split(":")
        port = int(port)

        if kwargs.get("auto_remote_debug", True):
            if remote_debugging.startswith(("localhost", "127.0.0.1")) and port == 0:
                port = tools.free_port()
        kwargs["remote_debugging"] = "%s:%s" % (remote_debugging, port)

        default_executable_path = None
        # for proxy that need authentication
        plugin_file = str(os.getpid()) + "-plugin."
        plugin_extension = "xpi"
        if navigator == CustomDriver.CHROME:
            default_executable_path = "chromedriver"
            plugin_extension = "crx"
        elif navigator == CustomDriver.FIREFOX:
            default_executable_path = "geckodriver"
        plugin_file += plugin_extension
        # auto calculation of executable_path
        auto_exe_path = kwargs.get("auto_exe_path", True)
        n_version = kwargs.get("n_version", None)
        executable_path = kwargs.get(
            "executable_path", default_executable_path if
            not auto_exe_path else None
        )

        proxy_val = kwargs.get("proxy_val", None)
        custom_proxy = CustomProxy(proxy_val)
        random_user_agent = kwargs.get("random_user_agent", True)
        ua = None

        if random_user_agent:
            ua = random.choice(CustomDriver.create_user_agent()[navigator])
        user_agent = kwargs.get("default_ua", kwargs.get("ua", ua))
        kwargs["default_ua"] = user_agent

        extensions = None

        kwargs["need_authentication"] = custom_proxy.need_authentication

        # package: kb_package.crawler.drivers.navigators.driver_manager
        driver_manager = getattr(importlib.import_module(
            "kb_package.crawler.drivers.%s.driver_manager"
            % navigator_str),
            "DriverManager")()

        options: "webdriver.ChromeOptions" = driver_manager.get_options(**kwargs)
        options.page_load_strategy = "eager"
        # desired_capabilities = driver_manager.get_desired_capabilities(**kwargs)
        extra_args = driver_manager.extra_args(**kwargs)

        if remote_debugging.endswith(("localhost", "127.0.0.1")) and port == 0:
            pass
        else:
            popen_args = list(set("--" + str(x) if not str(x).startswith("--") and "=" in str(x)
                                  else str(x) for x in options.arguments))
            popen_args += getattr(options, "_custom_argument", [])
            print([options.binary_location, *popen_args])
            browser = subprocess.Popen(
                [options.binary_location, *popen_args],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )

            REGISTERED.append(browser.pid)

        if custom_proxy:
            driver_proxy = Proxy()
            driver_proxy.proxy_type = ProxyType.MANUAL

            if custom_proxy.need_authentication:
                # installing my custom extension "Manifest KB_PACKAGE_BYPASS"
                v2_or_v3 = "v2" if navigator_str == "firefox" else "v3"
                plugin_path = os.path.join(CustomDriver.PLUGIN_DIR, v2_or_v3)
                plugin_file = os.path.join(CustomDriver.PLUGIN_DIR, plugin_file)
                assert "manifest.json" in os.listdir(plugin_path)
                import zipfile
                with zipfile.ZipFile(plugin_file, 'w') as zp:
                    for file in os.listdir(plugin_path):
                        with open(os.path.join(plugin_path, file)) as extra_plugin_file:
                            if file.lower() == "background.js":
                                zp.writestr("background.js", extra_plugin_file.read() % custom_proxy.get())
                            elif file.lower().endswith((".json", "js")):
                                zp.writestr(file, extra_plugin_file.read())
                plugin_file = os.path.realpath(plugin_file)
                extensions = [{"name": "Manifest KB_PACKAGE_BYPASS", "path": plugin_file}]
            else:
                for k in ["http_proxy", "sslProxy"]:
                    setattr(driver_proxy, k, str(custom_proxy))
            # failed to change window state to 'normal', current state is 'maximized'
            options.proxy = driver_proxy

        if auto_exe_path and executable_path is None:
            last_exception = None
            for executable_path in driver_manager.generate_webdriver_exe(
                    n_version=n_version):
                logger.info('got executable_path:', executable_path)
                try:
                    os.chmod(executable_path, 0o755)
                except (PermissionError, Exception):
                    pass
                try:
                    return CustomDriver._get_driver(
                        options=options,
                        executable_path=executable_path,
                        logger=logger,
                        extensions=extensions,
                        max_try=1,
                        sleep_time=0,
                        **extra_args
                    )
                except Exception as ex:
                    last_exception = ex
            raise last_exception

        else:
            return CustomDriver._get_driver(
                options=options,
                executable_path=executable_path,
                logger=logger,
                extensions=extensions,
                **extra_args
            )

    @staticmethod
    def _configure_headless(driver, info=print):
        pass


@atexit.register
def _exit():
    for pid in REGISTERED:
        try:
            os.kill(pid, signal.SIGTERM)
        except:
            pass


if __name__ == "__main__":
    d = CustomDriver(navigator="chrome", use_image=False)
    d.create()
    d.get("https://speed.hetzner.de/100MB.bin")
    time.sleep(10000)

