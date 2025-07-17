from datahub_integrations.chat.linkify import datahub_linkify


def test_datahub_linkify_dataset_link() -> None:
    text = "For [pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    expected = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    assert datahub_linkify(text) == expected


def test_datahub_linkify_multiple_dataset_links() -> None:
    text = "For [pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) and [pet_profiles_2](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles_2,PROD)) the owner is Donald Duck."
    expected = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) and [@pet_profiles_2](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles_2,PROD)) the owner is Donald Duck."
    assert datahub_linkify(text) == expected


def test_datahub_linkify_correct_dataset_link() -> None:
    text = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    expected = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    assert datahub_linkify(text) == expected


def test_datahub_linkify_dot_and_link() -> None:
    text = "For the MongoDB version [adoption.pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    expected = "For the MongoDB version [@adoption.pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    assert datahub_linkify(text) == expected


def test_datahub_linkify_container_link() -> None:
    text = "The [name](urn:li:container:b8ffe773179f6b1b1b5193419714dc19) schema has 25 tables."
    expected = "The [@name](urn:li:container:b8ffe773179f6b1b1b5193419714dc19) schema has 25 tables."
    assert datahub_linkify(text) == expected


def test_datahub_linkify_text_without_link() -> None:
    text = "For the MongoDB version urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD) the owner is Donald Duck."
    expected = "For the MongoDB version urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD) the owner is Donald Duck."
    # No change for only urn mentions yet
    assert datahub_linkify(text) == expected


def test_datahub_linkify_with_round_brackets() -> None:
    text = "This is a tableau datasource [datasource with bracket(1)](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket(1)](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert datahub_linkify(text) == expected


def test_datahub_linkify_with_incorrect_closing_bracket_in_link() -> None:
    text = "This is a tableau datasource [datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)] and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert datahub_linkify(text) == expected


def test_datahub_linkify_with_square_bracket_in_display_text_not_fixed() -> None:
    # Links with square brackets in display text are not auto-fixed
    text = "This is a tableau datasource [datasource with bracket[1]](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    expected = "This is a tableau datasource [datasource with bracket[1]](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert datahub_linkify(text) == expected


def test_datahub_linkify_with_missing_closing_bracket_in_link() -> None:
    text = "This is a tableau datasource [datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD) and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert datahub_linkify(text) == expected
