import requests
from .constants import *
from time import sleep


class CustomSession(requests.Session):
    """
    Create a custom session to add consistency delay on writes
    """

    def post(self, *args, **kwargs):
        response = super(CustomSession, self).post(*args, **kwargs)
        if "/logIn" not in args[0]:
            print("sleeping.")
            sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return response


def post(*args, **kwargs):
    response = requests.post(*args, **kwargs)
    if "/logIn" not in args[0]:
        print("sleeping.")
        sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
    return response


def get(*args, **kwargs):
    return requests.get(*args, **kwargs)
