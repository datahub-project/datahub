import pytest

from datahub_integrations.slack.utils.slackify import slackify_markdown


def test_slackify_markdown() -> None:
    text = "\n\nThis is a **bold** and _italic_ text.\nMy list:\n- item 1\n- item 2"
    expected = "This is a *bold* and _italic_ text.\nMy list:\n• item 1\n• item 2"
    assert slackify_markdown(text) == expected


def test_slackify_markdown_full_dataset_link() -> None:
    text = "For [pet_profiles](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet_profiles%2CPROD%29) the owner is Donald Duck."
    expected = "For <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet_profiles%2CPROD%29|pet_profiles> the owner is Donald Duck."
    assert slackify_markdown(text) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("This is a __bold__ text", "This is a *bold* text"),
        (
            "This is a __bold__ text with a link [__dataset__name__](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2C__dataset__name__%2CPROD%29).",
            "This is a *bold* text with a link <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2C__dataset__name__%2CPROD%29|__dataset__name__>.",
        ),
        (
            "and this is a link with dataset starting with double underscore [__pet_profiles](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2C__pet_profiles%2CPROD%29).",
            "and this is a link with dataset starting with double underscore <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2C__pet_profiles%2CPROD%29|__pet_profiles>.",
        ),
        (
            "and this is a link with dataset ending with double underscore [pet_profiles__](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet_profiles__%2CPROD%29).",
            "and this is a link with dataset ending with double underscore <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet_profiles__%2CPROD%29|pet_profiles__>.",
        ),
        (
            "and this is a link with double underscore [pet__profiles](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet__profiles%2CPROD%29).",
            "and this is a link with double underscore <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet__profiles%2CPROD%29|pet__profiles>.",
        ),
        (
            "and this is another link with double underscore [pet__profiles__complete](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet__profiles__complete%2CPROD%29).",
            "and this is another link with double underscore <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet__profiles__complete%2CPROD%29|pet__profiles__complete>.",
        ),
        (
            "and this is a link with triple underscore [pet___profiles](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet___profiles%2CPROD%29).",
            "and this is a link with triple underscore <https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cpet___profiles%2CPROD%29|pet___profiles>.",
        ),
    ],
)
def test_slackify_does_not_change_double_underscore_in_dataset_name(
    text: str, expected: str
) -> None:
    assert slackify_markdown(text) == expected
