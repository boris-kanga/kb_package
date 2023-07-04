from typing import Union, Optional
import re
from urllib.parse import quote as parse_url


class CustomProxy:
    def __init__(self, value: Optional[Union[str, dict, int]]):
        """
        value: Union[str, dict, int],
            -For custom_proxy which don't need authentication
                        don't mention user or password arg
                Examples: "127.0.0.1:24000" or "localhost:24000" or 24000 or
                            {"host":, "port":}
            -For custom_proxy which require authentication:
                    Examples: "user:pwd@host:port" or
                            {"host":, "pwd|password":, "user":, "port":}
        """
        self._value: dict = CustomProxy._parse(value)

    def __bool__(self):
        return self._value["port"] is not None

    @property
    def need_authentication(self) -> bool:
        return all([self._value[key] is not None for key in ["user", "pwd"]])

    def __getitem__(self, item):
        return self.get(item)

    def get(self, item: str = None) -> Optional[Union[str, dict]]:
        return ((self._value if self else None)
                if item is None else self._value.get(item, None))

    def __str__(self):
        if not self:
            return ""
        _string = ""
        if self.need_authentication:
            _string = "%s:%s@" % (self["user"], self["pwd"])
        return "%s%s:%s" % (_string, self["host"], self["port"])

    @staticmethod
    def _parse(value: Union[str, dict, int]) -> dict:
        if value is None:
            return {key: None for key in ["host", "port", "user", "pwd"]}
        assert isinstance(value, (dict, int, str)), "Bad value given"
        if isinstance(value, int):
            value = {"host": "127.0.0.1", "port": value}
        elif isinstance(value, str):

            res = re.match(r"([^:]+)(?::([^@]+)@([^:]+))?:(\d+)",
                           value)
            assert res is not None, f"Bad value value given:" \
                                    f" {value}. The right " \
                                    f"format is " \
                                    f"user:(pwd@host:)?port"
            if res.groups()[1] is None:
                host, _, _, port = res.groups()

                value = {"host": host, "port": port}
            else:
                user, pwd, host, port = res.groups()
                value = {
                    "user": user,
                    "pwd": pwd,
                    "port": port,
                    "host": host
                }
        assert "port" in value, "Bad value given: missing 'port' value"
        value["port"] = int(value["port"])
        assert value["port"] in range(1, 65535), "Port number must be in " \
                                                 "the range 1..65535"
        value["host"] = value.get("host", "127.0.0.1")
        value["host"] = ("127.0.0.1"
                         if value["host"] == "localhost"
                         else value["host"]
                         )
        value["pwd"] = parse_url(value.get("pwd", None) or
                                 value.get("password", None))
        value["user"] = parse_url(value.get("user", None) or
                                  value.get("username", None))

        return {key: value[key] for key in ["host", "port", "user", "pwd"]}


if __name__ == '__main__':
    value = "localhost:10"
    value = {
        "host": "localhost",
        "port": "222",
        "user": "kanga",
        "pwd": "hdhd@hdhd"
    }
    print(CustomProxy(None).get())
