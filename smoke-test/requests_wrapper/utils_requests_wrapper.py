import requests
from tests.consistency_utils import wait_for_writes_to_sync


class CustomSession(requests.Session):
    """
    Create a custom session to add consistency delay on writes
    """

    def post(self, *args, **kwargs):
        response = super(CustomSession, self).post(*args, **kwargs)
        if "/logIn" not in args[0]:
            print("sleeping.")
            wait_for_writes_to_sync()
        return response


def post(*args, **kwargs):
    response = requests.post(*args, **kwargs)
    if "/logIn" not in args[0]:
        print("sleeping.")
        wait_for_writes_to_sync()
    return response


def get(*args, **kwargs):
    return requests.get(*args, **kwargs)
