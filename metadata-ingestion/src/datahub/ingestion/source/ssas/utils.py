"""
Module for work with DNS
"""

import socket
from typing import List

from .tools import HostDefaults


class DNSHostNameResolver:
    """
    Class for work with dns response
    """

    def __init__(self, hostname: str, dns_suffix_list: List[str]):
        self.hostname = hostname
        self.dns_suffix_list = dns_suffix_list

    def __has_domain(self) -> bool:
        return len(self.hostname.split(".")) > 1

    @property
    def primary_hostname(self) -> str:
        """
        Get primary hostname

        :return: primary hostname as str
        """

        primary_host_name = self.hostname
        if self.__has_domain():
            primary_host_name = DNSHostName(
                hostname=self.hostname
            ).get_primary_server_name()
        else:
            for suffix in self.dns_suffix_list:
                try:
                    hostname = ".".join([self.hostname, suffix])
                    primary_host_name = DNSHostName(hostname=hostname).get_primary_server_name()
                    if primary_host_name:
                        break
                except Exception as e:
                    print(e)
        return primary_host_name


class DNSHostName:
    """
    Class for work with dns
    """

    def __init__(self, hostname: str):
        self.hostname = hostname
        self.primary_hostname: str = HostDefaults.NAME
        self.aliaslist: List[str] = []
        self.ipaddrlist: List[str] = []
        self.__parse_dns_info()

    def is_primary_name(self) -> bool:
        """
        Check is hostname as primary
        """
        return self.hostname == self.get_primary_server_name()

    def get_primary_host_name(self) -> str:
        """
        Get primary hostname
        """

        return self.primary_hostname

    def get_domain(self) -> str:
        """
        Extract domain from primary hostname

        :return: host domain as str
        """

        try:
            if self.primary_hostname:
                domain_part = self.primary_hostname.split(HostDefaults.SEPARATOR)[1:]
                if domain_part:
                    return HostDefaults.SEPARATOR.join(
                        self.primary_hostname.split(HostDefaults.SEPARATOR)[1:]
                    )
                return HostDefaults.DOMAIN
            raise ValueError
        except IndexError:
            return HostDefaults.DOMAIN
        except ValueError:
            return HostDefaults.DOMAIN

    def get_primary_server_name(self) -> str:
        """
        Get primary hostname
        """

        try:
            if self.primary_hostname:
                return self.primary_hostname.split(HostDefaults.SEPARATOR)[0]
            raise ValueError
        except IndexError:
            return HostDefaults.SERVER
        except ValueError:
            return HostDefaults.SERVER

    def is_alias(self) -> bool:
        """
        Check hostname is alias or not
        """

        return any([self.hostname in item for item in self.aliaslist])

    def __parse_dns_info(self) -> None:
        """
        Get dns request and parse responce
        """

        try:
            (
                self.primary_hostname,
                self.aliaslist,
                self.ipaddrlist,
            ) = socket.gethostbyname_ex(self.hostname)
        except Exception as excp:
            raise excp

    def get_aliases(self) -> List[str]:
        """
        Get hostname aliases list
        """

        return self.aliaslist

    def get_ipaddrlist(self) -> List[str]:
        """
        Get hostname ipaddress list
        """

        return self.ipaddrlist
