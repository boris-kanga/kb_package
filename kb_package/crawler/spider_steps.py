# -*- coding: utf-8 -*-
"""
The SpyderSteps Object.
"""

import importlib

import selenium.common.exceptions

from kb_package.crawler.steps.step import Step


class SpyderSteps:
    def __init__(self, steps: list, driver):
        """
        Constructor
        Args:
            steps: list of str
            driver: selenuim.webdriver | CustomDriver object
        """
        self._steps = steps
        self._driver = driver

    def click_on_checkbox(self, css_selector):
        """
        method for click on checkbox using css selector
        Args:
            css_selector: str

        Returns:
            bool, got?
        """
        try:
            got = 1 == self._driver.execute_script(
                "let check=document.querySelector(arguments[0]);\
                if (check==null) return 0; \
                if (!check.checked){check.scrollIntoView(); check.click();} \
                return 1;",
                css_selector,
            )
            return got
        except (selenium.common.exceptions.JavascriptException, Exception):
            return False

    def input_form(
        self, dict__css_selector__value, sleep_time_min=0, sleep_time_max=3000
    ):
        """
        Use for fill form
        Args:
            dict__css_selector__value: list, of dict,
                Examples: {"css_selector": "#id", "value":2}
            sleep_time_min: int, sleep time minimum in microsecond between two
                typing
            sleep_time_max: int, sleep time maximum in microsecond between two
                typing

        Returns:
            bool, got?
        """
        css_selector = [f["css_selector"] for f in dict__css_selector__value]
        values = [f["value"] for f in dict__css_selector__value]
        m, mm = sleep_time_min, sleep_time_max
        return (
            self._driver.execute_script(
                f"let css_selector=arguments[0], values=arguments[1],"
                f" good=0, m={m},"
                f"mm={mm} ;"
                + """const wait = ms =>newPromise(
                        resolve =>setTimeout(resolve, ms)
                    );
                    async function run(){
                       for(let i=0; i<name.length;i++){
                            await wait(Math.floor(Math.random() * mm) + m);
                            try{
                                document.querySelector(css_selector[i]
                                        ).value=values[i];
                                good++;
                            }catch(e){}
                        }
                        return good;
                    }
                    return run();""",
                css_selector,
                values,
            )
            == len(dict__css_selector__value)
        )

    def get_step(self, step):
        """
        Method for get step object
        Args:
            step: str, the name of step

        Returns:
            Step object
        """
        try:
            module = importlib.import_module(
                f"job_search_helper.utils.crawler.steps.{step}_step"
            )
            step_object = getattr(module, step.capitalize())(self._driver)
            return step_object
        except ImportError:
            return None

    def next_step_applicable(self):
        """
        use it for try to get the next applicable step
        Returns:
            Step object
        """

        for step in self._steps[::-1]:
            step_object = self.get_step(step)
            if step_object.is_applicable:
                return step_object

    def run_step(self, step):
        """
        use this for run a specific step
        Args:
            step: str

        Returns:
            step execution returns value
        """
        if isinstance(step, str):
            step = self.get_step(step)
        assert isinstance(step, Step), "fail to get step"

        return step.execute()
