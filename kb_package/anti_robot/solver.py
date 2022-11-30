# -*- coding: utf-8 -*-
"""
This module contains a list of Captcha solver with 2 basic
methods: _get_balance and _solve_.
We have two types of solver: human based and AI based.

"""
import json
import time
from json import dumps
from typing import Dict

import twocaptcha.api
from capmonster_python import (
    CapmonsterException,
    FuncaptchaTask,
    GeeTestTask,
    HCaptchaTask,
    ImageToTextTask,
    RecaptchaV2Task,
    RecaptchaV3Task,
)
from deathbycaptcha.deathbycaptcha import AccessDeniedException, HttpClient,\
    SocketClient
from requests import post
from twocaptcha import TwoCaptcha
from python_anticaptcha import AnticaptchaClient, \
    NoCaptchaTaskProxylessTask, ImageToTextTask

from . import endcaptcha as ec

from .base import CaptchaSolver

from ..tools import timer

try:
    from ..logger.customlogger import CustomLogger
except ImportError:
    import logging

    CustomLogger = logging.getLogger

_LOGGER_NAME = "CaptchaManager"


class TwoCaptchaSolver(CaptchaSolver):

    def __init__(
            self,
            api_key: str = None,
            default_timeout: int = 120,
            recaptcha_timeout: int = 600,
            polling_interval: int = 10,
            logger=CustomLogger(_LOGGER_NAME),
            category="Human based",
            **kwargs
    ):
        """
        Constructor of TwoCaptcha solver
        Args:
            api_key: TwoCaptcha API token
            default_timeout: arg for TwoCaptcha config
            recaptcha_timeout: arg for TwoCaptcha config
            polling_interval: arg for TwoCaptcha config
            logger: CustomLogger
        """

        super().__init__(logger=logger, category=category)
        config = {
            "apiKey": api_key,
            "defaultTimeout": default_timeout,
            "recaptchaTimeout": recaptcha_timeout,
            "pollingInterval": polling_interval,
        }
        self.solver = TwoCaptcha(**config)
        self.config = config
        self.kwargs = kwargs

    @timer(logger_name=_LOGGER_NAME)
    def _get_balance(self) -> float:
        """
        Get TwoCaptcha balance using TwoCaptcha dict config

        Returns:
            The balance

        """
        return self.solver.balance()

    @timer(logger_name=_LOGGER_NAME)
    def _solve_text_captcha(self, text: str, **kwargs):
        """
        Text captcha solver.
        Args:
            text: Text to decode.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Decoded text.
        """
        return self.solver.text(text, **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_image_to_text(self, image_path: str, **kwargs):
        """
        Image to Text solver.
        Args:
            image_path: Image path.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Decoded text.
        """
        return self.solver.normal(image_path, **kwargs)

    @timer(logger_name=_LOGGER_NAME, verbose=True)
    def _solve_recaptcha_v2(
            self,
            site_key: str,
            url: str,
            max_try: int = 2,
            logger=None,
            logger_exception=print,
            logger_info=print,
    ):
        """
        Static method for recaptcha solving
        Args:
            url: str, link for the web site
            site_key: Website site key
            max_try: int, max try when got fail, default 2
            logger: logging.getLoggerClass() | customLogger
            logger_exception: Function (Optional), for exception printing
            logger_info: Function (Optional), for info printing

        Returns:
            Token.
        """
        if hasattr(logger, "info"):
            logger_info = logger.info
        if hasattr(logger, "exception"):
            logger_exception = logger.exception

        for i in range(max_try):
            try:
                token = self.solver.recaptcha(sitekey=site_key, url=url)[
                    "code"
                ]
                return token
            except twocaptcha.api.ApiException as e:
                if "ERROR_ZERO_BALANCE" in str(e):
                    logger_info("[-] Got ERROR_ZERO_BALANCE")
                    return None
                else:
                    time.sleep(3)
            except Exception as e:
                logger_exception(e)
        logger_info("[-] The solver finally fail")

    @timer(logger_name=_LOGGER_NAME)
    def _solve_recaptcha_v3(self, site_key: str, url: str, *args, **kwargs):
        """
        Recaptcha V2 solver.
        Args:
            site_key: Website site key.
            url: Website url.
            *args: TwoCaptcha additionnal parameters. Set Official doc.
            **kwargs: Optinal TwoCaptcha additionnal parameters.

        Returns:
            Token.
        """
        return self.solver.recaptcha(
            sitekey=site_key, url=url, version="v3", *args, **kwargs
        )

    @timer(logger_name=_LOGGER_NAME)
    def _solve_fun_captcha(self, site_key: str, url: str, **kwargs):
        """
        Fun captcha solver.
        Args:
            site_key: Website site key.
            url: Website url.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.funcaptcha(sitekey=site_key, url=url, **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_h_captcha(self, site_key: str, url: str, **kwargs):
        """
        HCapctha solver.
        Args:
            site_key: Website site key.
            url: Website url.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution
        """
        return self.solver.hcaptcha(sitekey=site_key, url=url, **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_geet_test(self, gt: str, challenge: str, url: str, **kwargs):
        """
        GeetTest solver.
        Args:
            gt: gt
            challenge: challenge
            url: Website url.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.geetest(gt=gt, challenge=challenge, url=url,
                                   **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_key_captcha(
            self,
            user_id: str,
            session_id: str,
            web_server_sign_in: str,
            web_server_sign_in2: str,
            url: str,
            **kwargs,
    ):
        """
        KeyCaptcha solver.
        Args:
            user_id: User id.
            session_id: Session id.
            web_server_sign_in: Web server sign in.
            web_server_sign_in2: Web server sign in 2.
            url: Website url.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution
        """
        return self.solver.keycaptcha(
            s_s_c_user_id=user_id,
            s_s_c_session_id=session_id,
            s_s_c_web_server_sign=web_server_sign_in,
            s_s_c_web_server_sign2=web_server_sign_in2,
            url=url,
            **kwargs,
        )

    @timer(logger_name=_LOGGER_NAME)
    def _solve_capy_captcha(
            self, site_key: str, url: str, api_server: str, **kwargs
    ):
        """
        Capy captcha solver.
        Args:
            site_key: Website site key.
            url: Website url.
            api_server: API server.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.capy(
            sitekey=site_key, url=url, api_server=api_server, **kwargs
        )

    @timer(logger_name=_LOGGER_NAME)
    def _solve_grid_captcha(self, image_path: str, **kwargs):
        """
        Grid Captcha solver.
        Args:
            image_path: Image path.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.grid(image_path, **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_canvas_captcha(self, image_path: str, **kwargs):
        """
        Canvas captcha solver.
        Args:
            image_path: Image path.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.grid(image_path, **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_click_captcha(self, image_path: str, **kwargs):
        """
        Click captcha solver.
        Args:
            image_path: Image path.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.coordinates(image_path, **kwargs)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_rotate_captcha(self, image_path: str, **kwargs):
        """
        Rotate captcha solver.
        Args:
            image_path: Image path.
            **kwargs: TwoCaptcha additionnal parameters. Set Official doc.

        Returns:
            Captcha solution.
        """
        return self.solver.rotate(image_path, **kwargs)


class CapmonsterSolver(CaptchaSolver):
    """
    Captchamonster solver: Human based.
    """

    def __init__(
            self,
            api_key: str,
            logger=CustomLogger(_LOGGER_NAME),
            category="Human based",
    ):
        """
        Capmonster Captcha solver.
        Args:
            api_key: Capmonster API key.
            logger: Capmonster logger.
            category: Captcha category.

        References:
            https://github.com/alperensert/capmonster_python
        """
        super().__init__(logger=logger, category=category)

        self.api_key = api_key
        self.base_url = "https://api.capmonster.cloud"

    @timer(logger_name=_LOGGER_NAME)
    def _get_balance(self) -> float:
        """
        Get customer balance.
        Returns:
            Balance.
        """
        try:
            data = {"clientKey": self.api_key}
            url = self.base_url + "/getBalance"
            balance = json.loads(post(url=url, data=data).text)["balance"]
            return balance
        except Exception as ex:
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_image_to_text(self, image_path: str):
        """
        Solve Capmonster ImageToText.
        Args:
            image_path: ImageToText path

        Returns:
            Decoded text.
        """
        try:
            capmonster = ImageToTextTask(self.api_key)
            task_id = capmonster.create_task(image_path=image_path)
            result = capmonster.join_task_result(task_id).get("text")
            self.logger.info(f"CaptchaMonster ImageToTextSolver got {result}")
            return result
        except CapmonsterException as ex:
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_recaptcha_v2(self, site_key: str, url: str):
        """
        Solve Capmonster Recaptcha V2.
        Args:
            url: Website url.
            site_key: Website site key.

        Returns:
            Recaptcha V2 token.
        """
        try:
            capmonster = RecaptchaV2Task(self.api_key)
            task_id = capmonster.create_task(url, site_key)
            result = capmonster.join_task_result(task_id).get(
                "gRecaptchaResponse"
            )
            self.logger.info(f"CaptchaMonster RecaptchaV2Solver got {result}")
            return result
        except CapmonsterException as ex:
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_recaptcha_v3(self, site_key: str, url: str):
        """
        Solve Capmonster Recaptcha V3.
        Args:
            site_key: Website site key.
            url: Website url.

        Returns:
            Recaptcha V3 token.
        """
        try:
            capmonster = RecaptchaV3Task(self.api_key)
            task_id = capmonster.create_task(url, site_key)
            result = capmonster.join_task_result(task_id).get(
                "gRecaptchaResponse"
            )
            self.logger.info(f"CaptchaMonster RecaptchaV3Solver got {result}")
            return result
        except CapmonsterException as ex:
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_geet_test(self, gt: str, challenge: str, url: str):
        """
        Solve Capmonster GeetTask.
        Args:
            gt: gt param.
            challenge: Challenge param.
            url: Website url.

        Returns:
            GeetTask solution.
        """
        try:
            capmonster_python = GeeTestTask(self.api_key)
            task_id = capmonster_python.create_task(url, gt, challenge)
            result = capmonster_python.join_task_result(task_id)
            result = result.get("seccode")
            # print(result.get("challenge"))
            # print(result.get("validate"))
            self.logger.info(f"CaptchaMonster GeetSolver got {result}")
            return result
        except CapmonsterException as ex:
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_fun_captcha(
            self,
            url: str,
            public_key: str,
            proxy: str = None,
            port: int = None,
            ua: str = None,
    ):
        """
        Solve Capmonster FunCaptcha.
        Args:
            url: Website url.
            public_key: Website public key.
            proxy: Proxy value to use
            port: Port to use
            ua: User Agent to use.

        Returns:
            FunCptcha solution.
        """
        try:
            capmonster = FuncaptchaTask(self.api_key)
            capmonster.set_proxy("http", proxy, port)
            capmonster.set_user_agent(ua)
            task_id = capmonster.create_task(url, public_key)
            result = capmonster.join_task_result(task_id)
            result = result.get("token")
            self.logger.info(f"CaptchaMonster FunCaptchSolver got {result}")
            return result
        except CapmonsterException as ex:
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_h_captcha(
            self, url: str, public_key: str, proxy: str, port: int, ua: str
    ):
        """
        Solve Capmonster HCaptcha.
        Args:
            url: Website url.
            public_key: Website public key.
            proxy: Proxy value to use
            port: Port to use
            ua: User Agent to use.

        Returns:
            HCaptcha solution.
        """
        try:
            capmonster = HCaptchaTask(self.api_key)
            capmonster.set_proxy("http", proxy, port)
            capmonster.set_user_agent(ua)
            task_id = capmonster.create_task(url, public_key)
            result = capmonster.join_task_result(task_id)
            result = result.get("gRecaptchaResponse")
            self.logger.info(f"CaptchaMonster HCaptchaSolver got {result}")
            return result
        except CapmonsterException as ex:
            self.logger.exception(ex)


class DeathByCaptchaSolver(CaptchaSolver):
    """
    DeathByCaptcha solver.
    """

    def __init__(
            self,
            api_key: str,
            user: str,
            pwd: str,
            logger=CustomLogger(_LOGGER_NAME),
            category="Human based",
            **kwargs
    ):
        """
        DeathByCaptcha solver.
        Args:
            api_key: DeathByCaptcha API key.
            user: DeathByCaptcha username.
            pwd: DeathByCaptcha password.
            logger: Logger.
            category: Provider category.
        """
        self.category = category
        self.logger = logger
        super().__init__(logger=self.logger, category=self.category)
        self.kwargs = kwargs

        self.api_key = api_key
        self.user = user
        self.pwd = pwd
        try:
            self.client = HttpClient(user, pwd)
        except:
            self.client = SocketClient(user, pwd)

    @timer(logger_name=_LOGGER_NAME)
    def _get_balance(self) -> float:
        """
        Get account balance.
        Returns:
            Balance.
        """
        return self.client.get_balance()

    @timer(logger_name=_LOGGER_NAME)
    def _common_step(self, captcha_type: int, data: Dict):
        """
        Solver steps baseline.
        Args:
            captcha_type: Captcha type.
            data: Data related to current captcha type submission.

        Returns:
            Solver result.
        """
        json_captcha = dumps(data)
        try:
            captcha = self.client.decode(
                type=captcha_type, token_params=json_captcha
            )
            if captcha:
                return captcha["text"]
        except AccessDeniedException as ex:
            self.logger.warning(
                "error: Access to DBC API denied, "
                "check your credentials and/or balance"
            )
            self.logger.exception(ex)

    @timer(logger_name=_LOGGER_NAME)
    def _solve_recaptcha_v2(
            self, site_key: str, url: str, proxy: str = ""
    ):
        """
        RecaptchaV2 solver.
        Args:
            site_key: Website site key.
            url: Website url.
            proba: Probability of prediction.
            proxy: Example: 'http://user:password@127.0.0.1:1234'

        Returns:
            RecaptchaV2 result.
        """
        data = {
            "custom_proxy": proxy,
            "proxytype": "HTTP",
            "googlekey": site_key,
            "pageurl": url,
        }
        result = self._common_step(4, data)
        self.logger.info(f"DeathByCaptcha RecaptchaV2 got {result}")

        return result

    @timer(logger_name=_LOGGER_NAME)
    def _solve_recaptcha_v3(
            self, site_key: str, url: str, proxy: str, proba: float = 0.8
    ):
        """
        RecaptchaV3 solver.
        Args:
            site_key: Website site key.
            url: Website url.
            proxy: Example: 'http://user:password@127.0.0.1:1234'
            proba: Probability of prediction.

        Returns:
            RecaptchaV3 result.
        """
        data = {
            "custom_proxy": proxy,
            "proxytype": "HTTP",
            "googlekey": site_key,
            "pageurl": url,
            "action": "example/action",
            "min_score": proba,
        }
        result = self._common_step(5, data)
        self.logger.info(f"DeathByCaptcha RecaptchaV3 got {result}")

        return result

    @timer(logger_name=_LOGGER_NAME)
    def _solve_fun_captcha(self, url: str, public_key: str, proxy: str):
        """
        FunCaptcha solver.
        Args:
            public_key: Website site key.
            url: Website url.
            proxy: Example: 'http://user:password@127.0.0.1:1234'

        Returns:
            Solver result.
        """
        data = {
            "custom_proxy": proxy,
            "proxytype": "HTTP",
            "publickey": public_key,
            "pageurl": url,
        }
        result = self._common_step(6, data)
        self.logger.info(f"DeathByCaptcha FunCaptcha got {result}")

        return result

    @timer(logger_name=_LOGGER_NAME)
    def _solve_h_captcha(self, url: str, site_key: str, proxy: str):
        """
        HCaptcha solver.
        Args:
            site_key: Website site key.
            url: Website url.
            proxy: Example: 'http://user:password@127.0.0.1:1234'

        Returns:
            Solver result.
        """
        data = {
            "custom_proxy": proxy,
            "proxytype": "HTTP",
            "sitekey": site_key,
            "pageurl": url,
        }
        result = self._common_step(7, data)
        self.logger.info(f"DeathByCaptcha HCaptcha got {result}")

        return result


class EndCaptcha(CaptchaSolver):
    """
    EndCaptcha solver.
    """

    def __init__(
            self,
            api_key: str,
            user: str,
            pwd: str,
            verbose: bool = False,
            logger=CustomLogger("EndCaptcha"),
            category="AI based",
    ):
        """

        Args:
            api_key: API key, Emplty for this solver.
            user: Username
            pwd: Password.
            verbose: Verbose option
            logger: Solver logger.
            category: Solver category, default 'AI based'.
        """
        self.category = category
        self.logger = logger

        super().__init__(logger=self.logger, category=self.category)

        self.api_key = api_key
        self.user = user
        self.pwd = pwd
        self.client = ec.HttpClient(self.user, self.pwd)
        self.verbose = verbose

    def _get_balance(self) -> float:
        """
        Get balance.
        Returns:
            Balance.
        """
        if self.verbose:
            self.client.is_verbose = self.verbose
        balance = self.client.get_balance()

        return balance

    def _solve_image_to_text(self, image) -> str:
        """
        Solver ImageToText captcha.
        Args:
            image: Image file path.

        Returns:
            Captcha solution.
        """
        token = self.client.upload(image)["text"]

        return token

    def _solve_recaptcha_v2(
            self, site_key: str, url: str, proxy: str = "",
            proxy_type: str = ""
    ) -> str:
        """
        Solver Recaptcha V2.
        Args:
            site_key: Website site key.
            url: Website url.
            proxy: Proxy, default ''.
            proxy_type: Proxy type, default ''.

        Returns:
            Captcha solution.
        """
        token_dict = {
            "custom_proxy": proxy,
            "proxytype": proxy_type,
            "googlekey": site_key,
            "pageurl": url,
        }
        token_params = dumps(token_dict)
        token = self.client.decode(type=4, token_params=token_params)["text"]

        return token


class AntiCaptchaSolver(CaptchaSolver):

    def __init__(self,
                 api_key: str = None,
                 logger=CustomLogger(_LOGGER_NAME),
                 category="Human based",
                 **kwargs):
        super().__init__(logger, category)
        self.api_key = api_key
        self.kwargs = kwargs
        self.solver = AnticaptchaClient(self.api_key)

    @timer(logger_name=_LOGGER_NAME)
    def _get_balance(self) -> float:
        """
        Get TwoCaptcha balance using TwoCaptcha dict config

        Returns:
            The balance

        """
        return self.solver.getBalance()

    @timer(logger_name=_LOGGER_NAME)
    def _solve_recaptcha_v2(self, url, site_key, invisible, maximum_time=120):
        task = NoCaptchaTaskProxylessTask(
            website_url=url, website_key=site_key, is_invisible=invisible
        )
        job = self.solver.createTask(task)
        job.join(maximum_time=maximum_time)
        return job.get_solution_response()

    @timer(logger_name=_LOGGER_NAME)
    def _solve_image_to_text(self, filename):
        captcha_fp = open(filename, 'rb')
        task = ImageToTextTask(captcha_fp)
        job = self.solver.createTask(task)
        job.join()
        response = job.get_captcha_text()

        return response
