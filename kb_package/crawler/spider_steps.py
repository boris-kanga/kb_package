# -*- coding: utf-8 -*-
"""
The SpyderSteps Object.
"""

import importlib
import random
import time

import selenium.common.exceptions

from kb_package.crawler.steps.step import Step


class SpyderSteps:
    def __init__(self, driver, steps: list=None):
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
        self, dict__css_selector__value, sleep_time_min=0, sleep_time_max=1
    ):
        """
        Use for fill form
        Args:
            dict__css_selector__value: list, of dict,
                Examples: {"selector": "#id", "value":2}
            sleep_time_min: int, sleep time minimum in microsecond between two
                typing
            sleep_time_max: int, sleep time maximum in microsecond between two
                typing

        Returns:
            bool, got?
        """
        for field in dict__css_selector__value:
            is_numeric = issubclass(field.get("value").__class__, (int, float))

            value = str(field.get("value"))
            temp = ""
            for car in value:
                s = random.uniform(sleep_time_min, sleep_time_max)
                temp += car
                self._driver.execute_script("document.querySelector(arguments[0]).value= arguments[1]",
                                            field.get("selector"),
                                            temp if not is_numeric else float(temp))
                time.sleep(s)

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

    def get_data(self, selectors, block=None, multi=True):
        """
        Code to get data in the page source
        Args:
            selectors: selectors
            block: None or str(selector) if data is group on a selector so
                need to select all group first before getting the data
            multi: bool Getting multiple data ?

        Returns:
            return extracted data
        """
        for k in list(selectors.keys()):
            if selectors.get(k) is None:
                selectors.pop(k)

        data = self._driver.execute_script(
            """
            let selectors = arguments[0];
            let card = arguments[1];
            let multi = arguments[2];
            if (card){
                let cards = [];
                if (multi){
                    cards = [...document.querySelectorAll(card)];
                }else{
                    if (document.querySelector(card)){
                        cards = [document.querySelector(card)];
                    }
                }
                return cards.map(offer=>{
                    let data = {};
                    Object.keys(selectors).forEach(k=>{
                        let attr = "innerText"
                        let select = selectors[k];
                        let custom_attr = false;
                        if (typeof select === 'object' && select !== null){
                            attr = select["ATTR"];
                            custom_attr = select["CUSTOM_ATTR"];
                            select = select["SELECT"];
                        }
                        let elm = null;
                        if (select){
                            elm = offer.querySelector(select)
                        }else{
                            elm = offer;
                        }
                        if (elm){
                            if (custom_attr){
                                data[k] = (elm.getAttribute(attr)||"").trim();
                            }else{
                                data[k] = (elm[attr] || "").trim();
                            }
                        }else{
                            data[k] = null;
                        }

                    });
                    return data;
                });
            }else{
                let data = [];
                Object.keys(selectors).forEach(k=>{
                    let attr = "innerText"
                    let select = selectors[k];
                    let custom_attr = false;
                    if (typeof select === 'object' && select !== null){
                        attr = select["ATTR"];
                        custom_attr = select["CUSTOM_ATTR"];
                        select = select["SELECT"];
                    }
                    let cards = [];
                    if (multi){
                        cards = [...document.querySelectorAll(select)];
                    }else{
                        if (document.querySelector(select)){
                            cards = [document.querySelector(select)];
                        }
                    }
                    cards.forEach((elm, index)=>{
                            if (data[index]== undefined){
                                data.push({})
                            }
                            if (custom_attr){
                                data[index][k] = (elm.getAttribute(attr) || ""
                                    ).trim();
                            }else{
                                data[index][k] = (elm[attr] || "").trim();
                            }
                    })
                });
                return data;
            }
            """,
            selectors,
            block or 0,
            multi,
        )
        return data if multi else (data or [None])[0]
