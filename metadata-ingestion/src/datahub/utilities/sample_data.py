import pathlib
import warnings


def download_sample_data() -> pathlib.Path:
    """Download the bootstrap sample data file.

    .. deprecated::
        Use ``datahub.cli.datapack.loader.download_pack`` with the "bootstrap" pack instead.
    """
    warnings.warn(
        "download_sample_data() is deprecated. Use the datapack loader instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    from datahub.cli.datapack.loader import download_pack
    from datahub.cli.datapack.registry import get_pack

    pack = get_pack("bootstrap")
    entries = download_pack(pack)
    return entries[0].path
