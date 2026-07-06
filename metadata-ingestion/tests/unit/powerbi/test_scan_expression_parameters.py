from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    parse_dataset_parameters_from_expressions,
)


def test_extracts_text_parameter_value():
    expressions = [
        {
            "name": "ServerHostName",
            "expression": '"adb-574989277074981.1.azuredatabricks.net" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
        }
    ]
    assert parse_dataset_parameters_from_expressions(expressions) == {
        "ServerHostName": "adb-574989277074981.1.azuredatabricks.net"
    }


def test_ignores_shared_query_expressions():
    """Non-parameter shared queries (IsParameterQuery absent) must not be
    treated as parameter values."""
    expressions = [
        {
            "name": "Revenues",
            "expression": 'let\n    Source = Sql.Database("host", "DB"),\n    T = Source{[Schema="s",Item="t"]}[Data]\nin\n    T',
        },
        {
            "name": "Catalog",
            "expression": '"prod00_bonsai" meta [IsParameterQuery=true, Type="Text"]',
        },
    ]
    assert parse_dataset_parameters_from_expressions(expressions) == {
        "Catalog": "prod00_bonsai"
    }


def test_extracts_non_text_parameter_value():
    expressions = [
        {
            "name": "Port",
            "expression": '443 meta [IsParameterQuery=true, Type="Number"]',
        }
    ]
    assert parse_dataset_parameters_from_expressions(expressions) == {"Port": "443"}


def test_empty_or_none_expressions():
    assert parse_dataset_parameters_from_expressions([]) == {}
    assert parse_dataset_parameters_from_expressions(None) == {}
