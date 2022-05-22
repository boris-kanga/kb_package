# -*- coding: utf-8 -*-
"""
Luminati Proxy Manager tools thats handle both Luminati
"""
from json import loads
from logging import Logger, getLogger
from typing import Dict

from requests import get, post


class LuminatiProxyManager:
    def __init__(self, token: str, logger: Logger = getLogger("LPM")):
        """
        Luminati Proxy Manager tools thats handle both Luminati
        custom_proxy and external custom_proxy.
        Args:
            token: str, Luminati API token.
            logger: Logger object.

        """
        self.token = token
        self.logger = logger

    def _base_cmd(
        self,
        thematic: str = "zone",
        data: Dict = dict,
        zone: str = "datacenter",
        item: str = "cost",
        request_type: str = "GET",
        others_items: str = "",
    ):
        """
        Execute LPM base command using Python API.
        Args:
            thematic: str, thematic of Luminati API to request.
            data: Dict, data to post if POST request.
            zone: str, Luminati zone.
            item: str, item to get the requested information.
            request_type: str, GET or POST.
            others_items: str, Others item. Example: &country=COUNTRY
        Returns:
            response: Dict, List
        """
        headers = {"Authorization": f"Bearer {self.token}"}
        if request_type == "GET":
            cmd = f"https://luminati.io/api/{thematic}/{item}\
            {others_items}?zone={zone}"
            response = get(cmd, headers=headers)
        elif request_type == "POST":
            cmd = f"https://luminati.io/api/{thematic}/{item}\
            {others_items}?zone={zone}"
            response = post(cmd, headers=headers, data=data)
        else:
            raise NotImplementedError("Request type not implemented.")
        status = response.status_code
        if status == 200:
            response = response.content
        else:
            self.logger.warning(
                f'Bad response status code {status} for cmd curl "{cmd}"'
            )
        if request_type == "POST":
            self.logger.info(f"request return {response}")

        return response

    # TO DO: methode à peaufiner car plusieurs options pas très claires
    def get_n_available_ips(
        self,
        item: str = "count_available_ips",
        zone: str = "datacenter",
        others_items: str = "",
    ) -> int:
        """
        Get Luminati zone available IPs.
        Args:
            zone: str, Luminati zone. Accepted values: datacenter,
            static_res or isp
            item: str, item to get the requested information.
            others_items: str, Others item. Example: &country=COUNTRY,
            country as fr or france
        Returns:
            int, Number of gIPs.
        """
        response = self._base_cmd(
            zone=zone, item=item, request_type="GET", others_items=others_items
        )

        return loads(response.decode("utf-8")).get("count")

    def get_customer_total_balance(
        self,
        thematic: str = "customer",
        item: str = "balance",
        zone: str = "",
        others_items: str = "",
    ) -> int:
        """
        Get Luminati customer total balance = remained
        balance + amout spent in the current month.
        Args:
            thematic: str, thematic of Luminati API to request.
            zone: str, Luminati zone. Accepted values: datacenter,
            static_res or isp
            item: str, item to get the requested information.
            others_items: str, Others item. Example: &country=COUNTRY,
            country as fr or france
        Returns:
            int, Customer total balance.
        """
        response = self._base_cmd(
            thematic=thematic,
            item=item,
            zone=zone,
            request_type="GET",
            others_items=others_items,
        )

        return loads(response.decode("utf-8")).get("balance")

    def get_zone_status(
        self,
        thematic: str = "zone",
        item: str = "status",
        zone: str = "datacenter",
    ) -> str:
        """
        Get zone status.
        Args:
            thematic: str, thematic of Luminati API to request.
            zone: str, Luminati zone. Accepted values: datacenter,
            static_res or isp
            item: str, item to get the requested information.
        Returns:
            str, Zone status.
        """
        response = self._base_cmd(
            thematic=thematic, item=item, zone=zone, request_type="GET"
        )

        return loads(response.decode("utf-8")).get("status")

    def get_active_zones(
        self,
        thematic: str = "zone",
        item: str = "get_active_zones",
        zone: str = "",
    ) -> list:
        """
        Get zone status.
        Args:
            thematic: str, thematic of Luminati API to request.
            zone: str, Luminati zone. Accepted values: datacenter,
            static_res or isp
            item: str, item to get the requested information.
        Returns:
            list, Active zones. Template: [{"name":"ZONE1","type":"dc"}].
        """
        return self._base_cmd(
            thematic=thematic, item=item, zone=zone, request_type="GET"
        )

    def add_ip_to_whitelist(self, ip):
        data = {"ip": ip}
        self._base_cmd(
            thematic="zone", item="whitelist", request_type="POST", data=data
        )
