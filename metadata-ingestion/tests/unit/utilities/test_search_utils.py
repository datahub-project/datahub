# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.utilities.search_utils import (
    ElasticDocumentQuery,
    LogicalOperator,
    SearchField,
)


def test_simple_and_filters():
    query = (
        ElasticDocumentQuery.create_from()
        .group(LogicalOperator.AND)
        .add_field_match("field1", "value1")
        .add_field_match("field2", "value2")
        .end()
    )

    assert query.build() == '(field1:"value1" AND field2:"value2")'


def test_simple_or_filters():
    query = (
        ElasticDocumentQuery.create_from()
        .group(LogicalOperator.OR)
        .add_field_match("field1", "value1")
        .add_field_match("field2", "value2")
        .end()
    )

    assert query.build() == '(field1:"value1" OR field2:"value2")'

    # Use SearchFilter to create this query
    query = (
        ElasticDocumentQuery.create_from()
        .group(LogicalOperator.OR)
        .add_field_match(SearchField.from_string_field("field1"), "value1")
        .add_field_match(SearchField.from_string_field("field2"), "value2")
        .end()
    )
    assert query.build() == '(field1:"value1" OR field2:"value2")'


def test_simple_field_match():
    query: ElasticDocumentQuery = ElasticDocumentQuery.create_from(
        ("field1", "value1:1")
    )
    assert query.build() == 'field1:"value1\\:1"'

    # Another way to create the same query
    query = ElasticDocumentQuery.create_from()
    query.add_field_match("field1", "value1:1")
    assert query.build() == 'field1:"value1\\:1"'


def test_negation():
    query = (
        ElasticDocumentQuery.create_from()
        .group(LogicalOperator.AND)
        .add_field_match("field1", "value1")
        .add_field_not_match("field2", "value2")
        .end()
    )

    assert query.build() == '(field1:"value1" AND -field2:"value2")'


def test_multi_arg_create_from():
    query: ElasticDocumentQuery = ElasticDocumentQuery.create_from(
        ("field1", "value1"),
        ("field2", "value2"),
    )
    assert query.build() == '(field1:"value1" AND field2:"value2")'
