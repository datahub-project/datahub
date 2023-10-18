import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RequestWrapper:
    def __init__(self, status_forcelist: list, method_whitelist: list, retries: int = 5):
        self.retries = retries
        self.status_forcelist = status_forcelist
        self.method_whitelist = method_whitelist

    def create_session(self, protocol: str = 'https://'):
        session = requests.Session()
        retries = Retry(total=self.retries, status_forcelist=self.status_forcelist,
                        method_whitelist=self.method_whitelist)
        session.mount(protocol, HTTPAdapter(max_retries=retries))
        return session
