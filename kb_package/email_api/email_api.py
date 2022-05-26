# -*- coding: utf-8 -*-
"""
The Email parser API.
Use for all emailing operation
"""

import re
import smtplib
import ssl
from collections.abc import Iterable
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr


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
            header,
            sender_email,
            receivers_email,
            subject,
            bcc,
            password,
            attached_files=None,
            x_priority=1,
            smtp_type="SMTP_SSL",
    ):
        """

        Args:
            html_code: str, the html code to send
            header: str, the header
            sender_email: str, email_api of the sender, used for email_api
                server
                connection
            receivers_email: list|str, mails of receivers
            subject: str, the email_api subject
            bcc: str, BCC
            password: str, the password for email_api server connection
            attached_files: list, files to send with email_api
            x_priority: int, x_priority
            smtp_type: str, smtp_type default SMTP_SSL

        Returns:
            None
        """

        attached_files = [] if attached_files is None else attached_files
        receivers_email = (
            receivers_email
            if isinstance(receivers_email, list)
            else [receivers_email]
        )

        message = MIMEMultipart()
        # Create a multipart message and set headers
        message["From"] = formataddr((header, sender_email))
        message["To"] = ",".join(receivers_email)

        message["Subject"] = subject
        message["Bcc"] = bcc
        message["X-Priority"] = str(x_priority)

        message.attach(MIMEText(html_code, "html"))

        for file in attached_files:
            try:
                with open(file, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
            except FileNotFoundError:
                pass
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment; filename={}".format(file),
            )
            message.attach(part)

        text = message.as_string()
        context = ssl.create_default_context()
        with getattr(smtplib, smtp_type.upper())(
                EmailAPI.EMAIL_SERVER_HOST,
                EmailAPI.EMAIL_SERVER_PORT[smtp_type.upper()],
                **({} if smtp_type.upper() == "SMTP" else {"context":context})
        ) as server:
            server.login(sender_email, password)
            for receiver in receivers_email:
                server.sendmail(sender_email, receiver, text)

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
                            if not isinstance(truth_iterator, Iterable):
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


if __name__ == "__main__":
    params = {"nom": "TEST", "contenu": "OK"}

    EmailAPI.EMAIL_SERVER_HOST = "mail.kb-portfolio.tech"
    bcc = "admin@kb-portfolio.tech"
    password = "wN2_bkS13Pn5uHt"
    with open("templates/base.html") as file:
        html = file.read()

    print(EmailAPI.get_render(html, params=params))
    EmailAPI.send_email(EmailAPI.get_render(html, params=params),
                        "Contact",
                        bcc,
                        "kangaborisparfait@gmail.com",

                        "Contact",
                        bcc,
                        password, smtp_type="smtp")