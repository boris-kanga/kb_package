# -*- coding: utf-8 -*-
"""
Get the error in a html code using references errors list
"""
import os


from kb_package import tools


class ErrorHandler:
    DIRECTORY = os.path.dirname(__file__)
    ERROR_LIST_PATH = os.path.join(DIRECTORY, "error_set.json")

    @staticmethod
    def detect_error(html_code, check_dom_load=True):
        """
        Method which return error
        Args:
            html_code: str
            check_dom_load: bool, check if the dom is loaded

        Returns:
            dict, {"html", "code", "level"}
        """
        html_code = html_code.lower()

        error_list = tools.read_json_file(ErrorHandler.ERROR_LIST_PATH)

        for error in error_list:
            if "alias" in error:
                if error["alias"].lower() in html_code:
                    return error
            if error["html"].lower() in html_code:
                return error
        if check_dom_load:
            if len(html_code) < 200:
                return {"html": "Dom don't load", "code": None, "level": 2}
        return None
