import os
import re
import time
from kb_package.crawler.custom_driver import CustomDriver
from kb_package.tools import BasicTypes


class Create:
    SITE = {
        "jescanned.com": {
            "URL": "https://www.emailondeck.com/",
            "SITE_KEY": "6LdjXQkTAAAAADFGRY5gVKtvhGvh8B2eF3oc-Y4E",
            "SELECTOR": "#yne_input"
        },
        "5ubo.com": {"URL": 'https://temp-mail.org/fr',
                     "SELECTOR": "#mail"}
    }

    def __init__(self, driver: CustomDriver = None, **kwargs):
        if not isinstance(driver, CustomDriver):
            kwargs["auto_exe_path"] = kwargs.get("auto_exe_path", True)
            kwargs["headless"] = kwargs.get("headless", False)
            kwargs["random_user_agent"] = True
            kwargs["use_image"] = False

            driver = CustomDriver(**kwargs)
        self.driver = driver
        self.own_tabs = {}
        self.messages = []

    def get_email_address(self, domain="5ubo.com"):
        site = Create.SITE[domain]
        tab_id = self.driver.open_new_tab(self.driver(), switch_on=True, load_page=True, for_url=site["URL"])
        res = self.driver.sleep_until_load(max_try=10, css_selector=site["SELECTOR"], get_data=True, check_error=False)

        assert res["got"], res["error"]
        email = str(res["data"])
        while not BasicTypes.is_email(email):
            time.sleep(1)
            email = self.driver.js("""return document.querySelector(arguments[0]).value""",
                                   site["SELECTOR"])

        self.own_tabs[tab_id] = {"mail": email, "site": site, "domain": domain}
        return email

    def get_new_mails(self, content=True):
        tab_id = list(self.own_tabs.keys())[0]
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            res = self.driver.js(
                """
                    return [...document.querySelectorAll('.inbox-dataList li:not(.hide)')]
                            .map(email=>{
                                let response =  {
                                        "link": email.querySelector("a").href,
                                        "sender": email.querySelector(".inboxSenderName").innerText, 
                                        "addr": email.querySelector(".inboxSenderEmail").innerText,
                                        "objet": email.querySelector(".inboxSubject").innerText,
                                        "got_files": !email.querySelector(".attachment>a").classList.contains("hide")
                                    };
                                //email.remove();
                                return response;
                            })
                        """)
            if content and len(res):
                for d in res:
                    self.driver.get(d["link"])
                    print(self.driver().current_url)
                    date = self.driver.sleep_until_load(max_try=10, css_selector=".user-data-time-data", get_data=True)
                    d["date"] = date['data']
                    self.driver.screenshot_all_page(self.driver())
                    d["content"] = self.driver.sleep_until_load(max_try=10,
                                                                css_selector=".inbox-data-content-intro",
                                                                get_data=True)["data"]
                    print(d)
                    if d["got_files"]:
                        all_last_files = os.listdir(self.driver.get_download_folder)
                        attached_names = self.driver.js("""
                            return [...document.querySelectorAll('.webAttachments .attachmentLi:not(.hide)')].map(f=>{
                                        f.querySelector("a").click(); return f.innerText })""")
                        attached_names = [os.path.splitext(name)[0] + r'(?:\(\d+\))?' + os.path.splitext(name)[1]
                                          for name in attached_names]
                        print(attached_names)
                        while True:
                            files = [f for f in os.listdir(self.driver.get_download_folder) if f not in all_last_files
                                     and any([re.match(f, xx) is not None for xx in attached_names])]
                            if len(files) == attached_names:
                                attached_names = [os.path.join(self.driver.get_download_folder, f) for f in files]
                                break
                        d["attached_files"] = attached_names
            self.messages.extend(res)
            return res

    def delete(self):
        tab_id = list(self.own_tabs.keys())[0]
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            self.driver.js("""document.querySelector("#click-to-delete").click()""")

    def refresh(self):
        # click-to-refresh
        tab_id = list(self.own_tabs.keys())[0]
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            self.driver.js("""document.querySelector("#click-to-refresh").click()""")


if __name__ == '__main__':
    obj = Create()
    print(obj.get_email_address(), obj.get_new_mails())
    while True:
        method = input("method:")
        if hasattr(obj, method):
            d = getattr(obj, method)
            if callable(d):
                print(d())
            else:
                print(d)
