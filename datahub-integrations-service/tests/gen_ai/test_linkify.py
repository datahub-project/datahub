from datahub_integrations.chat.slackify import slackify_markdown
from datahub_integrations.gen_ai.linkify import (
    auto_fix_chat_links,
    auto_fix_entity_mention_links,
)


def test_slackify_markdown() -> None:
    text = "\n\nThis is a **bold** and _italic_ text.\nMy list:\n- item 1\n- item 2"
    expected = "This is a *bold* and _italic_ text.\nMy list:\n• item 1\n• item 2"
    assert slackify_markdown(text) == expected


def test_slackify_markdown_full_dataset_link() -> None:
    text = "For [pet_profiles](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet_profiles%2CPROD%29) the owner is Donald Duck."
    expected = "For <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet_profiles%2CPROD%29|pet_profiles> the owner is Donald Duck."
    assert slackify_markdown(text) == expected


def test_datahub_linkify_dataset_link() -> None:
    text = "For [pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    expected = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_multiple_dataset_links() -> None:
    text = "For [pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) and [pet_profiles_2](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles_2,PROD)) the owner is Donald Duck."
    expected = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) and [@pet_profiles_2](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles_2,PROD)) the owner is Donald Duck."
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_correct_dataset_link() -> None:
    text = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    expected = "For [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:bigquery,staging.long_tail_companions.adoption.pet_profiles,PROD)) the owner is Donald Duck."
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_dot_and_link() -> None:
    text = "For the MongoDB version [adoption.pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    expected = "For the MongoDB version [@adoption.pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)): Business Owner: Admin (DataHub Root User)"
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_container_link() -> None:
    text = "The [name](urn:li:container:b8ffe773179f6b1b1b5193419714dc19) schema has 25 tables."
    expected = "The [@name](urn:li:container:b8ffe773179f6b1b1b5193419714dc19) schema has 25 tables."
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_text_without_link() -> None:
    text = "For the MongoDB version urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD) the owner is Donald Duck."
    expected = "For the MongoDB version urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD) the owner is Donald Duck."
    # No change for only urn mentions yet
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_with_round_brackets() -> None:
    text = "This is a tableau datasource [datasource with bracket(1)](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket(1)](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_with_incorrect_closing_bracket_in_link() -> None:
    text = "This is a tableau datasource [datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)] and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert auto_fix_entity_mention_links(text) == expected


def test_datahub_linkify_with_square_bracket_in_display_text_not_fixed() -> None:
    # NOTE Known issue: Links with square brackets in display text are not auto-fixed
    text = "This is a tableau datasource [datasource with bracket[1]](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket[1]](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert auto_fix_entity_mention_links(text) == expected.replace(
        "[@datasource with bracket[1]]", "[datasource with bracket[1]]"
    )


def test_datahub_linkify_with_missing_closing_bracket_in_link() -> None:
    text = "This is a tableau datasource [datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD) and check if linkify works with link in link"
    expected = "This is a tableau datasource [@datasource with bracket](urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_name,PROD)) and check if linkify works with link in link"
    assert auto_fix_entity_mention_links(text) == expected


def test_auto_fix_urn_only_links_dataset() -> None:
    text = "For the MongoDB version [@pet_profiles](urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)) the owner is Donald Duck."
    expected = "For the MongoDB version [@pet_profiles](https://longtailcompanions.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amongodb%2Cadoption.pet_profiles%2CPROD%29/) the owner is Donald Duck."
    assert auto_fix_chat_links(text, "https://longtailcompanions.acryl.io") == expected


def test_auto_fix_urn_only_links_user() -> None:
    text = "The owner is [@user](urn:li:corpuser:john.doe)."
    expected = "The owner is [@user](https://company.acryl.io/user/urn%3Ali%3Acorpuser%3Ajohn.doe/)."
    assert auto_fix_chat_links(text, "https://company.acryl.io") == expected


def test_auto_fix_urn_only_links_multiple() -> None:
    text = """Based on my analysis of the data model, here are the tables typically joined with pet profiles:

1. [PETS](urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pets,PROD)) - Contains records of all pets considered for adoption. Joined using the profile_id field, which links to pet_profiles.

2. [ADOPTIONS](urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.adoptions,PROD)) - Contains records of all adoption attempts. Joined with pets table using pet_fk field, which indirectly connects to pet profiles.

3. [HUMANS](urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.humans,PROD)) - Contains information about adopters. Joined with adoptions table via human_fk field.

4. [PET_STATUS_HISTORY](urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_status_history,PROD)) - Tracks the historical status changes of pets. Joined using profile_id field.

5. [DOG_BREEDS](urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.dog_breeds,PROD)) - Reference table with breed information. Joined with pet profiles using the breed field.

6. [DOG_BREED_CHARACTERISTICS](urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.dog_breed_characteristics,PROD)) - Contains detailed characteristics of dog breeds. Joined with dog_breeds table.

These tables form a comprehensive data model for tracking pet profiles, their adoption history, and characteristics."""
    expected = """Based on my analysis of the data model, here are the tables typically joined with pet profiles:

1. [PETS](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.adoption.pets%2CPROD%29/) - Contains records of all pets considered for adoption. Joined using the profile_id field, which links to pet_profiles.

2. [ADOPTIONS](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.adoption.adoptions%2CPROD%29/) - Contains records of all adoption attempts. Joined with pets table using pet_fk field, which indirectly connects to pet profiles.

3. [HUMANS](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.adoption.humans%2CPROD%29/) - Contains information about adopters. Joined with adoptions table via human_fk field.

4. [PET_STATUS_HISTORY](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.analytics.pet_status_history%2CPROD%29/) - Tracks the historical status changes of pets. Joined using profile_id field.

5. [DOG_BREEDS](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.analytics.dog_breeds%2CPROD%29/) - Reference table with breed information. Joined with pet profiles using the breed field.

6. [DOG_BREED_CHARACTERISTICS](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Adbt%2Clong_tail_companions.analytics.dog_breed_characteristics%2CPROD%29/) - Contains detailed characteristics of dog breeds. Joined with dog_breeds table.

These tables form a comprehensive data model for tracking pet profiles, their adoption history, and characteristics."""
    assert auto_fix_chat_links(text, "https://company.acryl.io") == expected


def test_auto_fix_unquoted_urn_link() -> None:
    text = "For the MongoDB version [pet_profiles](https://longtailcompanions.acryl.io/dataset/urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)) the owner is Donald Duck."
    expected = "For the MongoDB version [pet_profiles](https://longtailcompanions.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amongodb%2Cadoption.pet_profiles%2CPROD%29/) the owner is Donald Duck."
    assert auto_fix_chat_links(text, "https://longtailcompanions.acryl.io") == expected


def test_auto_fix_wrong_subpath_in_link() -> None:
    text = "For the MongoDB version [pet_profiles](https://longtailcompanions.acryl.io/dataset/urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)) the owner is [Donald Duck](https://longtailcompanions.acryl.io/corpuser/urn:li:corpuser:donald.duck)."
    expected = "For the MongoDB version [pet_profiles](https://longtailcompanions.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amongodb%2Cadoption.pet_profiles%2CPROD%29/) the owner is [Donald Duck](https://longtailcompanions.acryl.io/user/urn%3Ali%3Acorpuser%3Adonald.duck/)."
    assert auto_fix_chat_links(text, "https://longtailcompanions.acryl.io") == expected


def test_auto_fix_wrong_subpath_in_link_with_quoted_urn() -> None:
    text = "For the MongoDB version [pet_profiles](https://longtailcompanions.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amongodb%2Cadoption.pet_profiles%2CPROD%29/) the owner is [Donald Duck](https://longtailcompanions.acryl.io/corpuser/urn%3Ali%3Acorpuser%3Adonald.duck)."
    expected = "For the MongoDB version [pet_profiles](https://longtailcompanions.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amongodb%2Cadoption.pet_profiles%2CPROD%29/) the owner is [Donald Duck](https://longtailcompanions.acryl.io/user/urn%3Ali%3Acorpuser%3Adonald.duck/)."

    assert (
        auto_fix_chat_links(text, "https://longtailcompanions.acryl.io")
    ) == expected
