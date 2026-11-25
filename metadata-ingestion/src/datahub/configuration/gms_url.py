"""Normalisation of the GMS URL a client is pointed at.

DataHub Cloud serves GMS under the ``/gms`` path of the instance hostname,
while the bare hostname is the frontend. Only ``/api/*`` and ``/openapi/*``
are forwarded from the frontend to GMS, so a client that builds any other
GMS path (``/entities``, ``/aspects``, ``/runs``, ``/config``, ...) against
the bare hostname gets the UI, or the frontend's own endpoint of the same
name, instead of GMS. ``fixup_gms_url`` rewrites a DataHub Cloud hostname
into the GMS URL and leaves every other URL untouched.

This module has no dependencies inside ``datahub`` so that it can be used
from ``DatahubClientConfig`` itself, which is what lets every holder of a
client config see the normalised value rather than only the REST emitter.
"""


def _ensure_valid_gms_url_acryl_cloud(url: str) -> str:
    if "acryl.io" not in url:
        return url
    if url.endswith(":8080"):
        url = url.replace(":8080", "")
    if url.startswith("http://"):
        url = url.replace("http://", "https://")
    if url.endswith("acryl.io"):
        url = f"{url}/gms"
    elif url.endswith("acryl.io/"):
        url = f"{url}gms"
    if url.endswith("acryl.io/api/gms"):
        url = url.replace("acryl.io/api/gms", "acryl.io/gms")

    return url


def fixup_gms_url(url: str) -> str:
    if url is None:
        return ""
    if url.endswith("/"):
        url = url.rstrip("/")
    url = _ensure_valid_gms_url_acryl_cloud(url)
    return url
