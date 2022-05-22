# -*- coding: utf-8 -*-
"""
Definition of `LPMPortCreator` object which is the Luminati
custom_proxy Manager.
It can be use to
    - start/stop luminati
        >>>LPMPortCreator.start()
        >>>LPMPortCreator.stop()

    - get availability port
        >>> LPMPortCreator.get_ports(working=True)

    - create luminati port using args range_, zone
        >>>LPMPortCreator.get_luminati_port("0-100", "datacenter", create=True)

    - create external port using proxies file or list of proxies
        >>>LPMPortCreator.get_external_proxies_config_using_file(\
            file_path="path_to_proxies_file/file.xls", create=True)
"""

import copy
import json
import os
import subprocess
from collections.abc import Iterable

import pandas
import requests

from kb_package import tools


class LPMPortCreator:
    MAX_PORT_NUMBER = 24000
    HOST = "127.0.0.1"
    PORT = 22999

    @staticmethod
    def get_logs(**kargs):
        """
        Get luminati logs
        Args:
            **kargs:
                skip: int, Number of requests to be skipped from the beginning
                limit: int, Maximum number of requests to be fetched
                search: str, regex expression for filter
                status_code: int, a specific status code
                port_from: int, Lower bound for port number
                port_to: int, Upper bound for port number

        Returns:
            list, List of dict

        """

        for key in kargs.keys():
            if key not in [
                "skip",
                "limit",
                "search",
                "status_code",
                "port_from",
                "port_to",
            ]:
                kargs.pop(key)
        params = "&".join("%s=%s" % (k, v) for k, v in kargs.items())
        uri = LPMPortCreator._get_base_url() + "logs"
        return json.loads(
            LPMPortCreator._base_command(uri, method="GET", params=params).text
        )

    @staticmethod
    def add_whitelist_ip(ips):
        """
        Add ips for the sandbox whitelist
        Args:
            ips: str|list, the ip(s) you want to add in whitelist

        Returns:
            None

        """
        if isinstance(ips, str):
            ips = [ips]
        uri = LPMPortCreator._get_base_url() + "add_whitelist_ip"
        data = {"ip": None}
        for ip in ips:
            data["ip"] = ip
            LPMPortCreator._base_command(uri, data=data.copy())

    @staticmethod
    def _get_base_url():
        """
        Use for getting base request url
        Returns:
            str

        """
        return (
            "http://"
            + LPMPortCreator.HOST
            + ":"
            + str(LPMPortCreator.PORT)
            + "/api/"
        )

    @staticmethod
    def get_status() -> bool:
        """
        Get Luminati Proxy Manager status.
        Returns:
            bool, True for luminati is working
        """
        cmd = "luminati --status"
        return "PID:" in subprocess.check_output(cmd, shell=True).decode(
            "utf-8"
        )

    @staticmethod
    def ban_ips(ips, ms=None, domain=None):
        """
        Global ban ips
        Args:
            ips: str|list, ips you want to ban
            ms: int, ban for how many time
            domain: str domain you want to ban for the ip

        Returns:
            None

        """
        if isinstance(ips, str):
            ips = [ips]
        data = {"ip": None}
        if ms is not None:
            data["ms"] = int(ms)
        if domain is not None:
            data["domain"] = domain
        uri = LPMPortCreator._get_base_url() + "banip"
        for ip in ips:
            data["ip"] = ip
            LPMPortCreator._base_command(uri, data=data.copy())

    @staticmethod
    def _base_command(uri, method="POST", **kargs):
        """
        Execute LPM base command using Python API.
        Args:
            uri: str, the url request
            method: str, (GET|POST|DELETE|PUT)
            **kargs:
                params: str, query string
                data: dict

        Returns:
            request.response

        """
        data = kargs.get("data", None)
        params = kargs.get("params", None)
        response = requests.request(
            method.lower(), url=uri, data=data, params=params
        )

        return response

    @staticmethod
    def start(auto_upgrade: bool = True):
        """
        Use to run luminati
        Args:
            auto_upgrade: bool, for luminati upgrade

        Returns:
            None

        """
        cmd = (
            "luminati "
            + ("--auto-upgrade" if auto_upgrade else "")
            + "--daemon"
        )
        subprocess.Popen(
            cmd,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            shell=True,
        ).wait()

    @staticmethod
    def stop():
        """
        Use to stop Luminati
        Returns:
            None
        """

        subprocess.Popen(
            "luminati --stop-daemon", shell=True, stdout=subprocess.PIPE
        ).wait()

    @staticmethod
    def get_external_proxies_port(
        proxies,
        name,
        user=None,
        pwd=None,
        list_format="host|port|user|pwd",
        str_separator=":",
        max_union=1,
        create=False,
    ):
        """
        Use to get/create port for external custom_proxy providers using list and
        the default external port config
        Args:
            proxies: list, list of proxies like (str|dict|list)
            name: str, the name of the external custom_proxy provider
            user: str, the global username
            pwd: str, the global password
            list_format: str, the format use to retrieve data in proxies list
            str_separator: str, if proxies is list of string, the separator of
                    all value is needed
            max_union: int, use to combine many custom_proxy int one port, default
                1, which mean no combinaison
            create: bool, for port creation

        Returns:
            list of <LPMPort> object
        """

        path_default_config = (
            "job_search_helper/utils/custom_proxy/port/"
            "port_configs/default_external_port_config.json"
        )

        LPMPortCreator.get_ports()
        init_port = LPMPortCreator.MAX_PORT_NUMBER

        ports = []
        config = []
        error = []
        default_external_port_config = tools.read_json_file(
            path_default_config
        )

        for p in proxies:
            if isinstance(p, dict):
                pass
            elif isinstance(p, list):
                p = {
                    i: p[index]
                    for index, i in enumerate(list_format.split("|", 3))
                }
            elif isinstance(p, str):
                p = p.replace("@", str_separator, 1)
                p = p.replace("\n", "")
                p = p.split(str_separator, 3)
                p = {
                    i: p[index]
                    for index, i in enumerate(list_format.split("|", 3))
                }
            else:
                error.append(p)
                continue

            if "port" not in p or ("host" not in p and "ip" not in p):
                error.append(p)
            else:

                p_ = {
                    "host": [p[i] for i in ["host", "ip"] if i in p][0],
                    "port": int(p["port"]),
                    "user": (
                        [p[i] for i in ["user", "username"] if i in p] + [None]
                    )[0]
                    or user,
                    "pwd": (
                        [p[i] for i in ["pwd", "password"] if i in p] + [None]
                    )[0]
                    or pwd,
                }
                config.append(p_)
                if len(config) == max_union:
                    init_port += 1
                    conf = copy.deepcopy(default_external_port_config)
                    conf["port"] = init_port
                    conf["ext_proxies"] = [
                        p["user"]
                        + ":"
                        + p["pwd"]
                        + "@"
                        + p["host"]
                        + ":"
                        + str(p["port"])
                        for p in config
                    ]
                    port = LPMPortCreator.LPMPort(conf, name)
                    if create:
                        port.create()
                    ports.append(port)
                    config = []
        return ports

    @staticmethod
    def get_external_proxies_config_using_file(
        file_path,
        name=None,
        user=None,
        pwd=None,
        list_format="host|port|user|pwd",
        str_separator=":",
        max_union=1,
        create=False,
    ):
        """
        Use to get/create port for external custom_proxy providers using file and
        the default external port config
        Args:
            file_path: str, the file you want to load.
                accepted format : {txt, csv, xls, xlsx}
            name: str, the name of the custom_proxy provider
            user: str, the global username
            pwd: str, the global password
            list_format: str, the format use to retrieve data in proxies list
            str_separator: str, if proxies is list of string, the separator of
                    all value is needed
            max_union: int, use to combine many custom_proxy int one port, default
                1, which mean no combinaison
            create: bool, for port creation

        Returns:
            list of <LPMPort> object
        """
        if name is None:
            name = os.path.splitext(os.path.basename(file_path))[0]
        extension = os.path.splitext(file_path)[1][1:].lower()
        file_format = {
            "xls": "excel",
            "xlsx": "excel",
            "csv": "csv",
            "txt": "txt",
        }
        assert (
            extension in file_format.keys()
        ), "This type of file is " "not supported: %s" % set(
            file_format.keys()
        )
        extension = file_format[extension]
        if extension == "txt":
            with open(file_path) as file:
                data = file.readlines()
        else:
            data = getattr(pandas, "read_" + extension)(file_path)
            data = [list(data.iloc[i]) for i in range(data.shape[0])]
        return LPMPortCreator.get_external_proxies_port(
            data,
            name,
            user=user,
            pwd=pwd,
            list_format=list_format,
            str_separator=str_separator,
            max_union=max_union,
            create=create,
        )

    @staticmethod
    def get_luminati_port(range_, zone, create=False):
        """
        Use to get/create luminati port for using the default port config
        Args:
            range_: (list|str|int) the range of port
            zone: str, ("datacenter"|"static_res"|...)
            create: bool, for port creation

        Returns:
            list of <LPMPort> object
        """
        path = (
            "job_search_helper/utils/custom_proxy/port/port_configs/"
            "default_luminati_port_config.json"
        )

        ports = []
        if isinstance(range_, str):
            range_ = [int(t) for t in range_.split("-", 2)]
            if range_[0] < 24000:
                range_[0] += 24000
                range_[1] += 24000
            range_ = range(range_[0], range_[1] + 1)
        if isinstance(range_, int):
            LPMPortCreator.get_ports()
            init_port = LPMPortCreator.MAX_PORT_NUMBER
            range_ = range(init_port + 1, init_port + 1 + range_)
        if isinstance(range_, Iterable):
            default_port_config = tools.read_json_file(path)
            for p in range_:
                conf = copy.deepcopy(default_port_config)
                conf["port"] = p
                conf["zone"] = zone
                port = LPMPortCreator.LPMPort(conf, "LUMINATI")
                if create:
                    port.create()
                ports.append(port)
        else:
            raise ValueError(f"Bad range {range_} given")

        return ports

    @classmethod
    def create_port_using_config(cls, config: dict):
        """
        Create a specific port using a config
        Args:
            config: dict, which specify port configuration

        Returns:
            bool, creation is it ok?
        """
        return cls.LPMPort(config).create()

    @staticmethod
    def modify_global_port_config(new_conf):
        """
        Modify all port config
        Args:
            new_conf: dict
                Examples:
                    new_conf = {"ssl": False}

        Returns:
            (str|list), "ok" if good
        """
        response = "ok"
        ports = LPMPortCreator.get_ports()
        for port in ports:
            if port.update(new_conf):
                pass
            else:
                if isinstance(response, str):
                    response = []
                response.append("fail update port :" + str(port.port))
                if len(response) == len(ports):
                    response = "All updating fails"

        return response

    @classmethod
    def get_ports(cls, working=False):
        """
        Get all port
        Args:
            working: bool, for only port which have good status

        Returns:
            list of <LPMPort>
        """
        uri = LPMPortCreator._get_base_url() + "proxies_running/"
        proxies = json.loads(cls._base_command(uri, method="GET").text)
        LPMPortCreator.MAX_PORT_NUMBER = max(proxies, key=lambda p: p["port"])[
            "port"
        ]
        data = [cls.LPMPort(proxy) for proxy in proxies]
        if working:
            data = [proxy for proxy in data if proxy.get_status()]
        return data

    class LPMPort:
        EQUALITY_CRITERIA = [
            "port",
            "zone",
            "country",
            "ssl",
            "rules",
            "headers",
            "ext_proxies",
            "preset",
            "rotate_session",
        ]

        def __init__(self, config: dict = None, name: str = None):
            """
            Constructor of LPMPort
            Args:
                config: dict, the port config
                name: str, the name of custom_proxy provider
            """
            self._config = config
            self._name = name
            if name is None:
                self._name = config.get("name", "LUMINATI")
            self.is_external = len(config.get("ext_proxies", [])) > 0

            if not any([config.get(k, None) for k in ["port"]]):
                raise ValueError("Bad port config given")
            self.port = config["port"]

            minimal_default_key = {"ssl": True, "rules": [], "headers": []}
            for key, value in minimal_default_key.items():
                self._config[key] = self._config.get(key, value)

        def __str__(self):
            """
            for port printing
            Returns:
                str, {port, zone, country, ssl, name}
            """
            d = {
                k: self._config.get(k, None)
                for k in ["port", "zone", "country", "ssl"]
            }
            d["name"] = self._name
            return str(d)

        def minimal_config(self):
            """
            Use for port comparaison
            Returns:
                dict,
            """
            return {
                k: self._config.get(k, None) for k in self.EQUALITY_CRITERIA
            }

        def __eq__(self, other):
            """
            Port comparaison
            Args:
                other: dict|LPMPort

            Returns:
                bool

            """
            if isinstance(other, dict):
                return {
                    k: other.get(k, None) for k in self.EQUALITY_CRITERIA
                } == self.minimal_config()
            elif isinstance(other, self.__class__):
                return self.minimal_config() == other.minimal_config()
            else:
                return False

        def create(self, force=False):
            """
            Port creation
            Args:
                force: bool, force port creation by deleting if exists the
                    port using the same port number

            Returns:
                bool, got?
            """
            try:
                port = self.get_port(self.port)
                if force or self != port:
                    port.delete_port()
                elif self == port:
                    return True
            except ValueError:
                pass
            uri = LPMPortCreator._get_base_url() + "proxies/"
            response = LPMPortCreator._base_command(uri=uri, data=self._config)
            return response.status_code == 200

        def __repr__(self):
            """
            For console representation
            Returns:
                str
            """
            add = ""
            if self._name != "LUMINATI":
                add = "(" + str(self._name) + ")"
            return "<LPMPort " + add + " : " + str(self) + ">"

        def get_status(self):
            """
            Test port status
            Returns:
                bool
            """
            uri = LPMPortCreator._get_base_url() + "proxy_status/{}".format(
                self.port
            )
            response = LPMPortCreator._base_command(uri=uri, method="GET")
            return response.text.lower() == "ok"

        def refresh_sessions(self):
            """
            Refresh port session
            Returns:
                None
            """
            uri = (
                LPMPortCreator._get_base_url()
                + "refresh_sessions/{}".format(self.port)
            )
            LPMPortCreator._base_command(uri=uri)

        def update(self, new_conf: dict):
            """
            Update port config
            Args:
                new_conf: dict, the new config

            Returns:
                bool, got?
            """
            self._config.update(new_conf)
            uri = LPMPortCreator._get_base_url() + "proxies/{}".format(
                self.port
            )
            response = LPMPortCreator._base_command(
                uri=uri, method="PUT", data=self._config
            )

            return response.status_code == 200

        def delete_port(self):
            """
            Delete the port
            Returns:
                bool, got?
            """
            uri = LPMPortCreator._get_base_url() + "proxies/{}".format(
                self.port
            )
            response = LPMPortCreator._base_command(uri=uri, method="DELETE")

            return response.status_code == 204

        @classmethod
        def get_port(cls, port_number):
            """
            Get port using port number
            Args:
                port_number: int

            Returns:
                LPMPort object
            Raises:
                ValueError: if port don't find

            """
            port_number = int(port_number)
            for p in LPMPortCreator.get_ports():
                if int(p.port) == port_number:
                    return p
            raise ValueError(f"{port_number} is Unknown")

        def get_ip(self, lpm_method=False):
            """
            Get the current navigation ip
            Args:
                lpm_method: bool, for Luminati method

            Returns:
                str, ip
            """
            if lpm_method:
                curl_ = (
                    "curl -s --custom_proxy "
                    + LPMPortCreator.HOST
                    + ':{} "http://lumtest.com/myip.json"'.format(self.port)
                )
                msg = json.loads(subprocess.check_output(curl_, shell=True))
                ip = msg["ip"]
            else:
                curl_ = (
                    "curl -s --custom_proxy "
                    + LPMPortCreator.HOST
                    + ':{} "http://ip.42.pl/raw"'.format(self.port)
                )

                ip = (
                    subprocess.check_output(curl_, shell=True).decode().strip()
                )
            return ip

        def ban_ip(self, ips=None, ms=None, domain=None, auto=True):
            """
            Ban ip for the port
            Args:
                ips: str, list
                ms: int for how many time
                domain: str, specific domain
                auto: for omitted ips arg and use the current navigation ip

            Returns:
                bool, got?
            Raises:
                AssertionError: when bad ip arg given
            """
            if auto:
                ips = self.get_ip()

            if isinstance(ips, str):
                ips = [ips]
            assert isinstance(ips, list), "Bad arg ip given"
            # print("going to ban", ips)
            uri = (
                LPMPortCreator._get_base_url() + "proxies/{}/"
                "banip".format(self.port)
            )
            data = {"ip": None}
            if ms is not None:
                data["ms"] = ms
            if domain is not None:
                data["domain"] = domain

            got = True
            for ip in ips:
                data = {"ip": ip}
                response = LPMPortCreator._base_command(
                    uri=uri, data=data.copy()
                )
                got = got and response.status_code == 204
            return got

        def unban_ip(self, ips):
            """
            Unban ip for the port
            Args:
                ips: str, list

            Returns:
                bool, got?
            Raises:
                AssertionError: when bad ip arg given
            """
            if isinstance(ips, str):
                ips = [ips]
            assert isinstance(ips, list), "Bad arg ip given"
            # print("going to unban", ips)
            uri = (
                LPMPortCreator._get_base_url()
                + "proxies/{}/unbanips".format(self.port)
            )
            response = LPMPortCreator._base_command(uri=uri, data={"ips": ips})
            return response.status_code == 200

        def ban_list(self):
            """
            Get port ban ip list
            Returns:
                list of ip
            """
            uri = LPMPortCreator._get_base_url() + "banlist/{}/".format(
                self.port
            )
            response = LPMPortCreator._base_command(uri=uri, method="GET")
            return json.loads(response.text)

        def get_logs(self, **kargs):
            """
            Get port logs
            Args:
                **kargs:
                    skip: int, Number of requests to be skipped from the
                            beginning
                    limit: int, Maximum number of requests to be fetched
                    search: str, regex expression for filter
                    status_code: int, a specific status code
            Returns:
                list, List of dict

            """
            kargs["port_to"] = self.port
            kargs["port_from"] = self.port
            return LPMPortCreator.get_logs(**kargs)
