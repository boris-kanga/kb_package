# -*- coding: utf-8 -*-
"""EndCapcha HTTP.

To access the HTTP API, use
HttpClient class.  Both are thread-safe.

HttpClient give you the following methods:

get_balance()
    Returns your EC account balance in US cents.

get_captcha(cid)
    Returns an uploaded CAPTCHA details as a dict with the following keys:

    "captcha": the CAPTCHA numeric ID; if no such CAPTCHAs found, it will
        be the only item with the value of 0;
    "text": the CAPTCHA text, if solved, otherwise None.

    The only argument `cid` is the CAPTCHA numeric ID.

get_text(cid)
    Returns an uploaded CAPTCHA text (None if not solved).  The only argument
    `cid` is the CAPTCHA numeric ID.

report(cid)
    Reports an incorrectly solved CAPTCHA.  The only argument `cid` is the
    CAPTCHA numeric ID.  Returns True on success, False otherwise.

upload(captcha)
    Uploads a CAPTCHA.  The only argument `captcha` can be either file-like
    object (any object with `read` method defined, actually, so StringIO
    will do), or CAPTCHA image file name.  On successul upload you'll get
    the CAPTCHA details dict (see get_captcha() method).

    NOTE: AT THIS POINT THE UPLOADED CAPTCHA IS NOT SOLVED YET!  You have
    to poll for its status periodically using get_captcha() or get_text()
    method until the CAPTCHA is solved and you get the text.

decode(captcha, timeout=DEFAULT_TIMEOUT)
    A convenient method that uploads a CAPTCHA and polls for its status
    periodically, but no longer than `timeout` (defaults to 60 seconds).
    If solved, you'll get the CAPTCHA details dict (see get_captcha()
    method for details).  See upload() method for details on `captcha`
    argument.

Visit http://www.endcaptcha.com for updates.

"""

from __future__ import unicode_literals

import hashlib
import imghdr
import os
import re
import time

import requests

try:
    # from json import read as json_decode
    # from json import write as json_encode
    pass
except ImportError:
    try:
        # from json import dumps as json_encode
        # from json import loads as json_decode
        pass
    except ImportError:
        # from simplejson import dumps as json_encode
        # from simplejson import loads as json_decode
        pass


# API version and unique software ID
API_VERSION = "EC/Python v1.0"

# Default CAPTCHA timeout and decode() polling interval
DEFAULT_TIMEOUT = 60
DEFAULT_TOKEN_TIMEOUT = 120
POLLS_INTERVAL = [1, 1, 2, 3, 2, 2, 3, 2, 2]
DFLT_POLL_INTERVAL = 3

# Base HTTP API url
HTTP_BASE_URL = "http://api.endcaptcha.com"

# Preferred HTTP API server's response content type, do not change
HTTP_RESPONSE_TYPE = "application/json"


def _load_image(captcha):
    if hasattr(captcha, "read"):
        img = captcha.read()
    else:
        img = ""
        try:
            captcha_file = open(captcha, "rb")
        except Exception:
            raise
        else:
            img = captcha_file.read()
            captcha_file.close()
    if not len(img):
        raise ValueError("CAPTCHA image is empty")
    elif imghdr.what(None, img) is None:
        raise TypeError("Unknown CAPTCHA image type")
    else:
        return img


class AccessDeniedException(Exception):
    pass


class Client(object):

    """EndCaptcha API Client."""

    def __init__(self, username, password):
        self.is_verbose = False
        self.userpwd = {"username": username, "password": password}

    def _log(self, cmd, msg=""):
        if self.is_verbose:
            print("%d %s %s" % (time.time(), cmd, msg.rstrip()))
        return self

    def get_balance(self):
        pass

    def get_captcha(self, cid):
        """Fetch a CAPTCHA details -- ID, text and correctness flag."""
        raise NotImplementedError()

    def get_text(self, cid):
        """Fetch a CAPTCHA text."""
        return self.get_captcha(cid).get("text") or None

    def report(self, cid):
        """Report a CAPTCHA as incorrectly solved."""
        raise NotImplementedError()

    def upload(self, captcha):
        """Upload a CAPTCHA.

        Accepts file names and file-like objects.  Returns CAPTCHA details
        dict on success.

        """
        raise NotImplementedError()

    def decode(self, captcha=None, timeout=None, **kwargs):
        """
        Try to solve a CAPTCHA.

        See Client.upload() for arguments details.

        Uploads a CAPTCHA, polls for its status periodically with arbitrary
        timeout (in seconds), returns CAPTCHA details if (correctly) solved.

        """
        if not timeout:
            if not captcha:
                timeout = DEFAULT_TOKEN_TIMEOUT
            else:
                timeout = DEFAULT_TIMEOUT

        deadline = time.time() + (max(0, timeout) or DEFAULT_TIMEOUT)
        uploaded_captcha = self.upload(captcha, **kwargs)
        cid = None

        if uploaded_captcha.get("error"):
            return uploaded_captcha

        if uploaded_captcha:
            intvl_idx = 0  # POLL_INTERVAL index
            while deadline > time.time() and not uploaded_captcha.get("text"):
                intvl, intvl_idx = self._get_poll_interval(intvl_idx)
                time.sleep(intvl)
                cid = uploaded_captcha.get("unsolved_yet")
                uploaded_captcha = self.get_captcha(cid)

            if uploaded_captcha.get("text"):
                if cid:
                    uploaded_captcha.update({"captcha_id": cid})

            return uploaded_captcha

        return None

    def _get_poll_interval(self, idx):
        """Returns poll interval and next index depending on index provided"""

        if len(POLLS_INTERVAL) > idx:
            intvl = POLLS_INTERVAL[idx]
        else:
            intvl = DFLT_POLL_INTERVAL
        idx += 1

        return intvl, idx


class HttpClient(Client):

    """EndCaptcha HTTP API client."""

    def __init__(self, *args):
        Client.__init__(self, *args)

    def _match_respose(self, response, cmd):
        res_match = re.match("^(ERROR):(.+)$|^(UNSOLVED_YET):(.+)$", response)

        if res_match:
            if res_match.groups()[0]:
                return {
                    res_match.groups()[0]
                    .lower(): res_match.groups()[1]
                    .lower()
                }
            elif res_match.groups()[2]:
                return {
                    res_match.groups()[2].lower(): res_match.groups()[3][6:]
                }

        else:
            if cmd == "balance":
                return {cmd: float(response)}
            if cmd[:6] == "/poll/" or cmd == "upload":
                return {"text": response}
            else:
                return {cmd: response}

    def _call(self, cmd, payload=None, headers=None, files=None):
        if headers is None:
            headers = {}
        if not payload:
            payload = {}
        headers["Accept"] = HTTP_RESPONSE_TYPE
        headers["User-Agent"] = API_VERSION
        self._log("SEND", "%s %d %s" % (cmd, len(payload), payload))
        if payload:
            response = requests.post(
                HTTP_BASE_URL + "/" + cmd.strip("/"),
                data=payload,
                files=files,
                headers=headers,
            )
        else:
            response = requests.get(
                HTTP_BASE_URL + "/" + cmd.strip("/"), headers=headers
            )
        status = response.status_code

        if 503 == status:
            raise OverflowError(
                "CAPTCHA was rejected due to service"
                " overload, try again later"
            )
        if not response.ok:
            raise RuntimeError("Invalid API response")
        self._log("RECV", "%d %s" % (len(response.text), response.text))
        try:
            return self._match_respose(response.text, cmd)
        except Exception:
            raise RuntimeError("Invalid API response")
        return {}

    def get_balance(self):
        return self._call("balance", self.userpwd.copy()) or {}

    def get_captcha(self, cid):
        return self._call("/poll/%s" % cid) or {"%s" % cid: 0}

    def report(self, captcha):
        payload = self.userpwd.copy()
        if isinstance(captcha, str) and captcha.isdigit():
            payload.update({"captcha_id": captcha})
        if isinstance(captcha, int):
            payload.update({"captcha_id": captcha})
        if isinstance(captcha, str) and re.match("^[0-9a-f]{40}$", captcha):
            payload.update({"hash": captcha})
        if (
            isinstance(captcha, str)
            and hasattr(captcha, "read")
            or isinstance(captcha, str)
            and os.path.isfile(captcha)
        ):
            image = _load_image(captcha)
            image_hash = hashlib.sha1(image).hexdigest()
            payload.update({"hash": image_hash})

        return self._call("report", payload)

    def upload(self, captcha=None, **kwargs):
        data = self.userpwd.copy()
        data.update(kwargs)
        files = {}

        if captcha:
            files = {"image": _load_image(captcha)}

        response = self._call("upload", payload=data, files=files) or {}

        return response
