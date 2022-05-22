# -*- coding: utf-8 -*-
"""
Base class that handle Captcha solver.
"""
from ..logger.customlogger import CustomLogger


class CaptchaSolver:
    """
    Captcha Solver base.
    """
    __methods__ = ["get_balance", "solve_text_captcha",
                   "solve_image_to_text",
                   "solve_recaptcha_v2", "solve_recaptcha_v3",
                   "solve_fun_captcha",
                   "solve_h_captcha", "solve_geet_test", "solve_key_captcha",
                   "solve_capy_captcha", "solve_grid_captcha",
                   "solve_canvas_captcha",
                   "solve_click_captcha", "solve_rotate_captcha"]

    def __init__(self, logger: CustomLogger, category: str, *args):
        self.logger = logger
        self.category = category

        for methods in CaptchaSolver.__methods__:
            if not hasattr(self, "_" + methods):
                setattr(self, "_" + methods,
                        lambda *args, method=methods, **kargs:
                        self._raise_not_implemented(
                            f"Method <{method}> not implemented for the "
                            "current captcha provider !"))

    def _raise_not_implemented(self, message):
        raise NotImplementedError(message)
