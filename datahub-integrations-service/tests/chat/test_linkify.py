from datahub_integrations.chat.linkify import linkify_slack


def test_linkify() -> None:
    base_url = "https://example.com"
    text1 = "For urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD) the owner is Donald Duck."
    text2 = "For the MongoDB version (urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    text3 = (
        "The urn:li:container:b8ffe773179f6b1b1b5193419714dc19 schema has 25 tables."
    )
    text4 = (
        "The `urn:li:container:b8ffe773179f6b1b1b5193419714dc19` schema has 25 tables."
    )
    text5 = "Full Path: <urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)|long_tail_companions.analytics.pet_details> Description"

    # TODO: Add a test for urns with spaces

    assert (
        linkify_slack(base_url, text1)
        == "For <https://example.com/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Abigquery%2Cstaging.long_tail_companions.adoption.pet_profiles%2CPROD%29/|staging.long_tail_companions.adoption.pet_profiles> the owner is Donald Duck."
    )
    assert (
        linkify_slack(base_url, text2)
        == "For the MongoDB version (<https://example.com/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amongodb%2Cadoption.pet_profiles%2CPROD%29/|adoption.pet_profiles>): Business Owner: Admin (DataHub Root User)"
    )
    assert (
        linkify_slack(base_url, text3)
        == "The <https://example.com/container/urn%3Ali%3Acontainer%3Ab8ffe773179f6b1b1b5193419714dc19/|urn:li:container:b8ffe773179f6b1b1b5193419714dc19> schema has 25 tables."
    )
    assert linkify_slack(base_url, text4) == text4

    assert (
        linkify_slack(base_url, text5)
        == "Full Path: <https://example.com/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.analytics.pet_details%2CPROD%29/|long_tail_companions.analytics.pet_details> Description"
    )
