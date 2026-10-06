from datahub.ingestion.source.azure.abs_utils import make_abs_urn
from datahub.utilities.urns.dataset_urn import DatasetUrn


def test_no_reserved_chars_unchanged() -> None:
    assert (
        make_abs_urn("https://acct.blob.core.windows.net/c/data/f.parquet", "PROD")
        == "urn:li:dataset:(urn:li:dataPlatform:abs,c/data/f_parquet,PROD)"
    )


def test_reserved_chars_encoded() -> None:
    urn = make_abs_urn(
        "https://acct.blob.core.windows.net/c/folder(1)/f.parquet", "PROD"
    )
    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:abs,c/folder%281%29/f_parquet,PROD)"
    )
    assert DatasetUrn.from_string(urn).name == "c/folder%281%29/f_parquet"
