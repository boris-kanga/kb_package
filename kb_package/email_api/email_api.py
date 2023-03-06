# -*- coding: utf-8 -*-
"""
The Email parser API.
Use for all emailing operation
"""
import os
import re
import smtplib
import ssl
from enum import Enum
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

import bs4

from kb_package.tools import BasicTypes, image_to_base64


class MAILPriority(Enum):
    # Plus haute importance
    HIGHEST = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    LOWEST = 5


class EmailAPI:
    REGEX_VARS = re.compile(r"{{ ([^}]*) }}", flags=re.S)
    REGEX_STRUCTURE = re.compile(
        r"{% (?P<struc>if|for) (.*?) %}(.*)" "{% end(?P=struc) %}", flags=re.S
    )

    EMAIL_SERVER_HOST = "vps76522.serveur-vps.net"
    EMAIL_SERVER_PORT = {"SMTP": 587, "SMTP_SSL": 465}

    @staticmethod
    def send_email(
            html_code,
            receivers_email,
            from_,
            password,
            subject=None,
            *,
            user=None,
            header=None,
            attached_files=None,
            cc=None,
            bcc=None,
            priority=MAILPriority.NORMAL,
            smtp_type="SMTP_SSL",
            host=EMAIL_SERVER_HOST,
            port=None
    ):
        """

        Args:
            html_code: str, the html code to send
            header: str, the header
            from_: str, email_api of the sender, used for email_api
                server
                connection
            receivers_email: list|str, mails of receivers
            subject: str, the email_api subject
            bcc: str, BCC
            cc: str
            password: str, the password for email_api server connection,
            user:
            attached_files: list, files to send with email_api
            priority: int, x_priority
            smtp_type: str, smtp_type default SMTP_SSL
            port:
            host:

        Returns:
            None
        """
        # html_code = EmailAPI.parse_final_email(html_code)

        attached_files = [] if attached_files is None else attached_files
        if not isinstance(attached_files, (list, tuple)):
            attached_files = [attached_files]
        else:
            attached_files = list(attached_files)
        receivers_email = (
            list(receivers_email)
            if isinstance(receivers_email, (list, tuple))
            else [receivers_email]
        )

        message = MIMEMultipart()
        # Create a multipart message and set headers
        message["From"] = formataddr((header, from_))
        message["To"] = ",".join(receivers_email)

        message["Subject"] = subject
        if cc is not None:
            cc = list(cc) if isinstance(cc, (tuple, list)) else [cc]
            message["Cc"] = ",".join(cc)
            receivers_email.extend(cc)
        if bcc is not None:
            bcc = list(bcc) if isinstance(bcc, (tuple, list)) else [bcc]
            message["Bcc"] = ",".join(bcc)
            receivers_email.extend(bcc)

        message["X-Priority"] = str(priority)

        message.attach(MIMEText(html_code, "html"))

        for index, file in enumerate(attached_files):
            try:
                if isinstance(file, dict):
                    name = file.get("name", "attached_file_" + str(index + 1) + os.path.splitext(file.get("path"))[1])
                    file = file.get("path")
                elif isinstance(file, str):
                    name = os.path.basename(file)
                else:
                    file = str(file)
                    name = os.path.basename(file)
                with open(file, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        "attachment; filename={}".format(name),
                    )
                    message.attach(part)
            except (FileNotFoundError, OSError, Exception):
                pass

        text = message.as_string()
        context = ssl.create_default_context()
        with getattr(smtplib, smtp_type.upper())(
                host or EmailAPI.EMAIL_SERVER_HOST,
                port or EmailAPI.EMAIL_SERVER_PORT[smtp_type.upper()],
                **({} if smtp_type.upper() == "SMTP" else {"context": context})
        ) as server:
            if smtp_type.upper() == "SMTP":
                server.starttls()
            if user is None:
                user = from_
            server.login(user, password)
            server.sendmail(from_, receivers_email, text)

    @staticmethod
    def get_render_from_file(file, params=None):
        """
        Get html code parsed
        Args:
            file: str, path for the template
            params: dict, for template elements values

        Returns:
            str, final html code
        """
        with open(file) as file:
            return EmailAPI.get_render(file.read(), params=params)

    @staticmethod
    def _get_vars(text):
        """
        Parse the text for get all vars like {{ ... }} specify in
        Args:
            text: str

        Returns:
            list
        """
        var = EmailAPI.REGEX_VARS.findall(text)
        return var

    @staticmethod
    def get_structures(text, params=None):
        """
        Parse the text to get the final html structure by removing tags
            {% if ... %} and {% for ... %}
        Args:
            text: str
            params: dict, for template elements values

        Returns:
            str, intermediate html code
        """
        return EmailAPI._get_structures(text, params)

    @staticmethod
    def eval_str(string, params=None):
        """
        Eval a str value using params
        Args:
            string: str, string to evaluate
            params: dict for params

        Returns:
            evaluation value
        """
        if params is None:
            params = {}
        for k, v in params.items():
            locals()[k] = v
        return eval(string)

    @staticmethod
    def _get_structures(text, params=None):
        """
        Parse the text to get the final html structure by removing tags
            {% if ... %} and {% for ... %}
        Args:
            text: str
            params: dict, for template elements values

        Returns:
            str, intermediate html code
        """
        regex = re.compile("({% (?:if|for) .*? %})", flags=re.S)
        structures = regex.split(text)
        text_reformatted = ""
        last_structure_pattern_start = re.compile(
            "^{% (if|for) (.*?) %}$", flags=re.S
        )
        last_structure_pattern_end = None
        current_working_text = {"struc": None, "object": None, "code": ""}
        number_of_clone = 0
        max_clone = 0
        # print("strucs", structures)
        my_strucs = []
        for s in structures:
            s_start = last_structure_pattern_start.match(s)
            if last_structure_pattern_end is not None:
                s_end = last_structure_pattern_end.findall(s)
            else:
                s_end = False
            if s_start and current_working_text["struc"] is None:
                s_start = s_start.groups()
                current_working_text["struc"] = s_start[0]
                current_working_text["object"] = s_start[1]
                current_working_text["code"] = ""
                end_pattern = "{% (endfor) %}"
                if s_start[0] == "if":
                    end_pattern = "{% (endif|else) %}"

                last_structure_pattern_end = re.compile(
                    rf"(.*?){end_pattern}", flags=re.S
                )
                number_of_clone = 0
                max_clone = 0
            elif s_end and current_working_text["struc"] is not None:
                index = 0
                # print("s--", s)
                # print("list end", s_end)
                # print(number_of_clone)
                add_code = ""
                for index, end in enumerate(s_end):
                    if (
                            current_working_text["struc"].lower() == "if"
                            and end[1] == "endif"
                    ) or "if" != current_working_text["struc"].lower():
                        number_of_clone -= 1
                    if (
                            number_of_clone == 0
                            and current_working_text["struc"].lower() == "if"
                            and end[1] == "else"
                    ):
                        number_of_clone = -1

                    # print("end", index, "=", end, "number_of_clone==",
                    # number_of_clone)
                    add_code += end[0]
                    # print(add_code)
                    if number_of_clone < 0:
                        # print("finish")
                        if current_working_text["struc"].lower() == "if":
                            current_working_text["object"] = EmailAPI.eval_str(
                                current_working_text["object"], params
                            )
                            if current_working_text["object"]:
                                break
                            else:
                                current_working_text["code"] = ""
                                add_code = ""
                                if end[1] == "else":
                                    current_working_text["object"] = True
                                    number_of_clone = 0
                                else:
                                    break
                        else:
                            break
                    else:
                        add_code += f"{'{% ' + end[1] + ' %}'}"
                # Calculation of str
                if number_of_clone < 0:
                    current_working_text["code"] += add_code
                    r_text = ""
                    # print(current_working_text)
                    if current_working_text["struc"].lower() == "if":
                        # "(.*){% else %}(.*)"
                        if current_working_text["object"]:
                            r_text = EmailAPI._get_structures(
                                current_working_text["code"], params=params
                            )
                    elif current_working_text["struc"].lower() == "for":
                        (
                            iterator_object,
                            iterable_object,
                        ) = current_working_text["object"].split(" in ")
                        iterable_object = EmailAPI.eval_str(
                            iterable_object, params=params
                        )
                        if "(" in iterator_object and ")" in iterator_object:
                            iterator_object = re.search(
                                r"\((.*?)\)", iterator_object
                            ).groups()[0]
                        iterator_object = iterator_object.split(",")
                        for truth_iterator in iterable_object:
                            if not BasicTypes.is_iterable(truth_iterator):
                                truth_iterator = [truth_iterator]
                            params_copy = {}
                            for index, key in enumerate(iterator_object):
                                params_copy[key] = truth_iterator[index]
                            t = EmailAPI.get_render(
                                current_working_text["code"],
                                params=params_copy,
                                ignore_error=False,
                            )
                            r_text += t
                    text_reformatted += r_text

                    ##
                    my_strucs.append(current_working_text)
                    p = re.compile(
                        f"{'{% end' + current_working_text['struc'] + ' %}'}",
                        flags=re.S,
                    )

                    p = "".join(p.split(s)[index + 1:])
                    text_reformatted += p

                    current_working_text = {
                        "struc": None,
                        "object": None,
                        "code": "",
                    }
                    last_structure_pattern_end = None
                else:
                    current_working_text["code"] += s
            else:
                if s_start and current_working_text["struc"] is not None:
                    s_start = s_start.groups()
                    if s_start[0] == current_working_text["struc"]:
                        number_of_clone += 1
                        max_clone += 1
                if current_working_text["struc"] is None:
                    text_reformatted += s
                else:
                    current_working_text["code"] += s
        return text_reformatted

    @staticmethod
    def get_render(text, params: dict = None, ignore_error: bool = True):
        """
        Get html code parsed
        Args:
            text: str, path for the template
            params: dict, for template elements values
            ignore_error: bool, error bad evaluation
        Returns:
            str, final html code
        """
        if params is None:
            params = {}
        params.update({"upper": str.upper, "lower": str.lower})
        for k, v in params.items():
            locals()[k] = v

        text = EmailAPI._get_structures(text, params=params)
        variables = EmailAPI._get_vars(text)
        for var in variables:
            try:
                text = text.replace("{{ " + var + " }}", str(eval(var)), 1)
            except (NameError, Exception):
                if ignore_error:
                    text = text.replace("{{ " + var + " }}", str(None), 1)

        return text

    @staticmethod
    def parse_final_email(html_code):
        html_code = bs4.BeautifulSoup(html_code, "lxml")
        for img in html_code.find_all("img"):
            src = img.attrs["src"]
            if BasicTypes.is_link(src):
                pass
            else:
                try:
                    img.attrs["src"] = image_to_base64(src)
                except FileNotFoundError:
                    pass
        return str(html_code)


if __name__ == "__main__":
    """
    params = {"nom": "TEST", "contenu": "OK"}

    EmailAPI.EMAIL_SERVER_HOST = "<host>"
    bcc = "<email_admin>"
    password = "<password>"
    with open("templates/base.html") as file:
        html = file.read()

    print(EmailAPI.get_render(html, params=params))
    EmailAPI.send_email(EmailAPI.get_render(html, params=params),
                        "Contact",
                        bcc,
                        "<receiver>",
                        "Contact",
                        bcc,
                        password, smtp_type="smtp")

    """
    EmailAPI.send_email("Juste un test",
                        "kangaborisparfait@gmail.com",
                        "parfait.kanga@orange.com",
                        password="<Password>",
                        user="",
                        header="",
                        subject="",
                        smtp_type="smtp",
                        attached_files=[],
                        host="192.168.4.161", port=25)