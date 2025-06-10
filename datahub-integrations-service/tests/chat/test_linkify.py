from datahub_integrations.chat.linkify import datahub_linkify


def test_datahub_linkify() -> None:
    text1 = "For [pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    text2 = "For the MongoDB version [adoption.pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    text3 = "The [name](urn:li:container:b8ffe773179f6b1b1b5193419714dc19) schema has 25 tables."
    text4 = "For the MongoDB version urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD) the owner is Donald Duck."

    assert (
        datahub_linkify(text1)
        == "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    )
    assert (
        datahub_linkify(text2)
        == "For the MongoDB version [@adoption.pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    )
    assert (
        datahub_linkify(text3)
        == "The [@name](urn:li:container:b8ffe773179f6b1b1b5193419714dc19) schema has 25 tables."
    )
    # No change for only urn mentions yet
    assert (
        datahub_linkify(text4)
        == "For the MongoDB version urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD) the owner is Donald Duck."
    )
