# -*- coding: utf-8 -*-
"""
Main class we will use to send captcha requests.
It included many captcha providers in order to reduce dependance.

References:
    - https://pypi.org/project/2captcha-python/#normal-captcha
    - https://www.deathbycaptcha.com/api
    - https://github.com/alperensert/capmonster_python


Required packages:
    deathbycaptcha~=0.1.1
    TwoCaptcha~=0.0.1
    2captcha-python==1.0.3
    python-anticaptcha==0.6.0
    capmonster_python
"""
import re
import string
import inspect

try:
    from ..logger.customlogger import CustomLogger
except ImportError:
    import logging

    CustomLogger = logging.getLogger

from . import solver


class CaptchaManager:
    """
    Captcha manager main class to solve any captcha type.
    """
    SOLVE_METHODS = [method for method in solver.CaptchaSolver.__methods__
                     if method != "get_balance"]

    ACCEPTED_TYPES = ["".join([p.capitalize() for p in method.split("_")[1:]])
                      for method in SOLVE_METHODS]

    ACCEPTED_PROVIDERS = [name.split("Solver")[0]
                          for name, _
                          in inspect.getmembers(
                            solver,
                            lambda member:
                            inspect.isclass(member) and member.__module__ ==
                            solver.__name__
                            )
                          ]

    def __init__(
            self,
            api_key: str,
            provider: str = "DeathbyCaptcha",
            logger=CustomLogger("CaptchaManager"),
            captcha_type: str = None,
            **kwargs
    ):

        # Validation
        if captcha_type is not None:
            captcha_type = self.get_captcha_type(captcha_type)
            if captcha_type is None:
                raise NotImplementedError(
                    f"Captcha type not in accepted values:"
                    f" {CaptchaManager.ACCEPTED_TYPES}"
                )
        provider = {t.lower(): t
                    for t in CaptchaManager.ACCEPTED_PROVIDERS}.get(
            provider.replace("_", "").lower(), None)

        if provider is None:
            raise NotImplementedError(
                f"Captcha Provider not in accepted values: "
                f"{CaptchaManager.ACCEPTED_PROVIDERS}"
            )

        user = kwargs.get("user", None)
        pwd = kwargs.get("pwd", None)
        self.api_key = api_key
        self.user = user
        self.pwd = pwd
        self.captcha_type = captcha_type
        self.provider = provider
        self.logger = logger
        self.kwargs = kwargs
        self.kwargs["logger"] = logger
        self.kwargs["api_key"] = api_key
        # Definition of timer logger name
        solver._LOGGER_NAME = logger.name
        self.provider_error_msg = (
            f"{self.provider} Provider not supported for this method !"
        )
        self.solver = getattr(solver, provider + "Solver")(**self.kwargs)

    @staticmethod
    def get_captcha_type(item: str):
        if item in CaptchaManager.SOLVE_METHODS:
            captcha_type = "_" + item
        elif item in [p.split("solve_")[1]
                      for p in CaptchaManager.SOLVE_METHODS]:
            captcha_type = "_solve_" + item
        else:
            captcha_type = item.replace("_", "")
            captcha_type: str = {t.lower(): t
                                 for t in CaptchaManager.ACCEPTED_TYPES}.get(
                captcha_type.lower(), None)
            if captcha_type is not None:
                captcha_type = "".join([p.lower()
                                        if p not in string.ascii_uppercase
                                        else "_" + p.lower() for p in
                                        re.split("([A-Z])", captcha_type)
                                        if len(p)])

                captcha_type = "_solve" + captcha_type
        return captcha_type

    def solve(self, *args, captcha_type=None, **kwargs):
        if captcha_type is not None:
            self.captcha_type = self.get_captcha_type(captcha_type)
        assert self.captcha_type is not None, "The params :captcha_type " \
                                              "not needed"
        try:
            return getattr(self.solver, self.captcha_type)(*args, **kwargs)
        except NotImplementedError as ex:
            self.logger.exception(f"{ex} {self.provider_error_msg}")

    def __call__(self, *args, **kwargs):
        return self.solve(*args, **kwargs)

    def __getattr__(self, item):
        if "balance" in item:
            item = "_get_balance"
        if item in self.solver.__methods__ or item == "_get_balance":
            try:
                return getattr(self.solver, item)
            except NotImplementedError as ex:
                self.logger.exception(f"{ex}, {self.provider_error_msg}")
        else:
            raise AttributeError(f"'{self.provider}->Solver'"
                                 f"has no attribute '{item}'")
