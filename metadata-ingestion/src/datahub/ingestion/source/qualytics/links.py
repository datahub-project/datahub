"""Deep links back into the Qualytics UI.

Every assertion carries an ``externalUrl`` to its container in Qualytics, so a DataHub
user looking at a failed assertion is one click from the checks and anomalies behind
it. Without it the metadata is a dead end: DataHub says something is wrong and offers
no way to go and look.

Route shapes match the links the Qualytics UI itself generates, not invented ones.
"""

from urllib.parse import urlsplit, urlunsplit


def derive_ui_base_url(api_base_url: str) -> str:
    """Strip the API root path off ``base_url`` to get the UI origin.

    A default Qualytics deployment serves its UI from the scheme and host of its API:
    ``https://acme.qualytics.io/api`` serves its UI from ``https://acme.qualytics.io``.
    Deployments that split the two can override this with ``ui_base_url``.
    """
    parts = urlsplit(api_base_url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def container_url(ui_base_url: str, datastore_id: int, container_id: int) -> str:
    return f"{ui_base_url}/datastores/{datastore_id}/containers/{container_id}/overview"
