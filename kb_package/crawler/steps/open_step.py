# -*- coding: utf-8 -*-
"""
Step open link
"""
from .step import Step


class Open(Step):
    def execute(self, url):
        """

        Args:
            url: str, url

        Returns:

        """
        self._driver.get(url)
