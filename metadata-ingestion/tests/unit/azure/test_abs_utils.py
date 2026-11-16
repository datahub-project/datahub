import pytest

from datahub.ingestion.source.azure.abs_utils import make_abs_urn
from datahub.utilities.urns.dataset_urn import DatasetUrn


def test_no_reserved_chars_unchanged() -> None:
    assert (
        make_abs_urn("https://acct.blob.core.windows.net/c/data/f.parquet", "PROD")
        == "urn:li:dataset:(urn:li:dataPlatform:abs,c/data/f_parquet,PROD)"
    )


@pytest.mark.parametrize(
    "abs_uri, expected_name",
    [
        (
            "https://acct.blob.core.windows.net/c/folder(1)/f.parquet",
            "c/folder%281%29/f_parquet",
        ),
        # The extension itself can hold reserved characters.
        ("https://acct.blob.core.windows.net/c/data/f.par(1)", "c/data/f_par%281%29"),
    ],
)
def test_reserved_chars_encoded(abs_uri: str, expected_name: str) -> None:
    urn = make_abs_urn(abs_uri, "PROD")
    assert urn == f"urn:li:dataset:(urn:li:dataPlatform:abs,{expected_name},PROD)"
    assert DatasetUrn.from_string(urn).name == expected_name
