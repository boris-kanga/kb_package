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

    def __init__(self, driver: CustomDriver=None, **kwargs):
        if not isinstance(driver, CustomDriver):
            kwargs["auto_exe_path"] = kwargs.get("auto_exe_path", True)
            kwargs["headless"] = kwargs.get("headless", True)

            driver = CustomDriver(**kwargs)
        self.driver = driver
        self.own_tabs = {}
        self.messages = []

    def get_email_address(self, domain="5ubo.com"):
        site = Create.SITE[domain]
        tab_id = self.driver.open_new_tab(self.driver(), switch_on=True, load_page=True, for_url=site["URL"])
        print("cici")
        self.driver.screenshot_all_page(self.driver())
        res = self.driver.sleep_until_load(max_try=10, css_selector=site["SELECTOR"], get_data=True)

        assert res["got"], res["error"]
        email = res["data"]
        while not BasicTypes.is_email(email):
            time.sleep(1)
            email = self.driver.execute_script("""return document.querySelector(arguments[0]).value""",
                                               site["SELECTOR"])

            print("got res", email)
        self.own_tabs[tab_id] = {"mail": email, "site": site, "domain": domain}
        return email

    def check_if_email_receive(self, tab_id):
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            return self.driver.execute_script("""

            let mails = document.querySelectorAll('a.title-subject');

            for(let i=0; i<mails.length; i++){
                let a = mails[i];
                if (a.innerText.trim() !=""){
                    if (a.innerText.includes("rendez-vous en attente")) return {text:a.innerText, url:a.href};
                }
            }
            return null;
            """)

    def get_new_mails(self):
        tab_id = list(self.own_tabs.keys())[0]
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            res = self.driver.execute_script(
                    """
                    return [...document.querySelectorAll('.inbox-dataList li:not(.hide)')]
                            .map(email=>{
                                return {
                                        "sender": email.querySelector(".inboxSenderName"), 
                                        "addr": email.querySelector(".inboxSenderEmail"),
                                        "objet": email.querySelector(".inboxSubject")
                                    };
                            })
                        """)
            self.messages.extend(res)
            return res

    def delete(self):
        tab_id = list(self.own_tabs.keys())[0]
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            self.driver.execute_script("""document.querySelector("#click-to-delete").click()""")

    def refresh(self):
        # click-to-refresh
        tab_id = list(self.own_tabs.keys())[0]
        self.driver.switch_to.window(tab_id)
        if self.own_tabs[tab_id]["domain"] == "5ubo.com":
            self.driver.execute_script("""document.querySelector("#click-to-refresh").click()""")


if __name__ == '__main__':
    obj = Create()
    print(obj.get_email_address(), obj.get_new_mails())
