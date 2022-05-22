# -*- coding: utf-8 -*-
"""
Click on next logic
"""
import selenium.common.exceptions
from selenium.common.exceptions import TimeoutException

from .step import Step


class ClickOnNext(Step):
    def wait_server_response_when_click_on_next(self, css_selector):
        """
        Wait for dom unload
        Args:
            css_selector: str, css selector for click button

        Returns:
            bool, got?
        """

        code_js = """
             const wait = ms =>new Promise(resolve =>setTimeout(resolve, ms));
             window.$$rdv_en_prefecture_waiter=true;
             async function run(){
             var btn=document.querySelector(arguments[0]);
             if (btn==null) return 0;
              btn.scrollIntoView(); btn.click();
              let stop=(window.$$rdv_en_prefecture_waiter)? false: true;
                let time_pause_ms=0;
                while (!stop){
                    time_pause_ms++;
                    stop=(window.$$rdv_en_prefecture_waiter)? false: true;
                    await wait(1);
                }
                return time_pause_ms;
             }
         return run();
        """
        got = True
        try:
            self._driver.execute_script(code_js, css_selector)
        except TimeoutException:
            got = False
        except selenium.common.exceptions.JavascriptException:
            pass
        return got

    def execute(
        self, css_selector, wait_for=None, max_dom_load_time_waiting=3
    ):
        """

        Args:
            css_selector: str, css selector for click button
            wait_for: str, criteria for dom loaded judgment
            max_dom_load_time_waiting: int, time in second for wait for
                dom loaded

        Returns:
            bool, got ?
        """

        got = self.wait_server_response_when_click_on_next(css_selector)
        if got:
            args = {"max_try": max_dom_load_time_waiting}
            if wait_for is not None:
                args["css_selector"] = wait_for
            return self.sleep_until_load(**args)
        return got
