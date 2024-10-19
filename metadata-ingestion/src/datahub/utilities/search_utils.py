import logging
import re
from enum import Enum
from typing import Generic, List, Optional, Tuple, Type, TypeVar, Union

logger = logging.getLogger(__name__)


class LogicalOperator(Enum):
    AND = "AND"
    OR = "OR"


class SearchField:
    def __init__(self, field_name: str):
        self.field_name = field_name

    def get_search_value(self, value: str) -> str:
        return value

    def __str__(self) -> str:
        return self.field_name

    def __repr__(self) -> str:
        return self.__str__()

    @classmethod
    def from_string_field(cls, field_name: str) -> "SearchField":
        return cls(field_name)


class QueryNode:
    def __init__(self, operator: Optional[LogicalOperator] = None):
        self.operator = operator
        self.children: List[Union[QueryNode, str]] = []

    def add_child(self, child: Union["QueryNode", str]) -> None:
        self.children.append(child)

    def build(self) -> str:
        if not self.children:
            return ""

        if self.operator is None:
            return (
                self.children[0]
                if isinstance(self.children[0], str)
                else self.children[0].build()
            )

        child_queries = []
        for child in self.children:
            if isinstance(child, str):
                child_queries.append(child)
            else:
                child_queries.append(child.build())

        joined_queries = f" {self.operator.value} ".join(child_queries)
        return f"({joined_queries})" if len(child_queries) > 1 else joined_queries


class ElasticsearchQueryBuilder:
    SPECIAL_CHARACTERS = r'+-=&|><!(){}[]^"~*?:\/'

    def __init__(self, operator: LogicalOperator = LogicalOperator.AND) -> None:
        self.root = QueryNode(operator=operator)

    @classmethod
    def escape_special_characters(cls, value: str) -> str:
        """
        Escape special characters in the search term.
        """
        return re.sub(f"([{re.escape(cls.SPECIAL_CHARACTERS)}])", r"\\\1", value)

    def _create_term(
        self, field: SearchField, value: str, is_exact: bool = False
    ) -> str:
        escaped_value = self.escape_special_characters(field.get_search_value(value))
        field_name: str = field.field_name
        if is_exact:
            return f'{field_name}:"{escaped_value}"'
        return f"{field_name}:{escaped_value}"

    def add_field_match(
        self, field: SearchField, value: str, is_exact: bool = True
    ) -> "ElasticsearchQueryBuilder":
        term = self._create_term(field, value, is_exact)
        self.root.add_child(term)
        return self

    def add_field_not_match(
        self, field: SearchField, value: str, is_exact: bool = True
    ) -> "ElasticsearchQueryBuilder":
        term = f"-{self._create_term(field, value, is_exact)}"
        self.root.add_child(term)
        return self

    def add_range(
        self,
        field: str,
        min_value: Optional[str] = None,
        max_value: Optional[str] = None,
        include_min: bool = True,
        include_max: bool = True,
    ) -> "ElasticsearchQueryBuilder":
        min_bracket = "[" if include_min else "{"
        max_bracket = "]" if include_max else "}"
        min_val = min_value if min_value is not None else "*"
        max_val = max_value if max_value is not None else "*"
        range_query = f"{field}:{min_bracket}{min_val} TO {max_val}{max_bracket}"
        self.root.add_child(range_query)
        return self

    def add_wildcard(self, field: str, pattern: str) -> "ElasticsearchQueryBuilder":
        wildcard_query = f"{field}:{pattern}"
        self.root.add_child(wildcard_query)
        return self

    def add_fuzzy(
        self, field: str, value: str, fuzziness: int = 2
    ) -> "ElasticsearchQueryBuilder":
        fuzzy_query = f"{field}:{value}~{fuzziness}"
        self.root.add_child(fuzzy_query)
        return self

    def add_boost(
        self, field: str, value: str, boost: float
    ) -> "ElasticsearchQueryBuilder":
        boosted_query = f"{field}:{value}^{boost}"
        self.root.add_child(boosted_query)
        return self

    def group(self, operator: LogicalOperator) -> "QueryGroup":
        return QueryGroup(self, operator)

    def build(self) -> str:
        return self.root.build()


class QueryGroup:
    def __init__(self, parent: ElasticsearchQueryBuilder, operator: LogicalOperator):
        self.parent = parent
        self.node = QueryNode(operator)
        self.parent.root.add_child(self.node)

    def add_field_match(
        self, field: Union[str, SearchField], value: str, is_exact: bool = True
    ) -> "QueryGroup":
        if isinstance(field, str):
            field = SearchField.from_string_field(field)
        term = self.parent._create_term(field, value, is_exact)
        self.node.add_child(term)
        return self

    def add_field_not_match(
        self, field: Union[str, SearchField], value: str, is_exact: bool = True
    ) -> "QueryGroup":
        if isinstance(field, str):
            field = SearchField.from_string_field(field)
        term = f"-{self.parent._create_term(field, value, is_exact)}"
        self.node.add_child(term)
        return self

    def add_range(
        self,
        field: str,
        min_value: Optional[str] = None,
        max_value: Optional[str] = None,
        include_min: bool = True,
        include_max: bool = True,
    ) -> "QueryGroup":
        min_bracket = "[" if include_min else "{"
        max_bracket = "]" if include_max else "}"
        min_val = min_value if min_value is not None else "*"
        max_val = max_value if max_value is not None else "*"
        range_query = f"{field}:{min_bracket}{min_val} TO {max_val}{max_bracket}"
        self.node.add_child(range_query)
        return self

    def add_wildcard(self, field: str, pattern: str) -> "QueryGroup":
        wildcard_query = f"{field}:{pattern}"
        self.node.add_child(wildcard_query)
        return self

    def add_fuzzy(self, field: str, value: str, fuzziness: int = 2) -> "QueryGroup":
        fuzzy_query = f"{field}:{value}~{fuzziness}"
        self.node.add_child(fuzzy_query)
        return self

    def add_boost(self, field: str, value: str, boost: float) -> "QueryGroup":
        boosted_query = f"{field}:{value}^{boost}"
        self.node.add_child(boosted_query)
        return self

    def group(self, operator: LogicalOperator) -> "QueryGroup":
        new_group = QueryGroup(self.parent, operator)
        self.node.add_child(new_group.node)
        return new_group

    def end(self) -> ElasticsearchQueryBuilder:
        return self.parent


SF = TypeVar("SF", bound=SearchField)


class ElasticDocumentQuery(Generic[SF]):
    def __init__(self) -> None:
        self.query_builder = ElasticsearchQueryBuilder()

    @classmethod
    def create_from(
        cls: Type["ElasticDocumentQuery[SF]"],
        *args: Tuple[Union[str, SF], str],
    ) -> "ElasticDocumentQuery[SF]":
        instance = cls()
        for arg in args:
            if isinstance(arg, SearchField):
                # If the value is empty, we treat it as a wildcard search
                logger.info(f"Adding wildcard search for field {arg}")
                instance.add_wildcard(arg, "*")
            elif isinstance(arg, tuple) and len(arg) == 2:
                field, value = arg
                assert isinstance(value, str)
                if isinstance(field, SearchField):
                    instance.add_field_match(field, value)
                elif isinstance(field, str):
                    instance.add_field_match(
                        SearchField.from_string_field(field), value
                    )
                else:
                    raise ValueError("Invalid field type {}".format(type(field)))
        return instance

    def add_field_match(
        self, field: Union[str, SearchField], value: str, is_exact: bool = True
    ) -> "ElasticDocumentQuery":
        if isinstance(field, str):
            field = SearchField.from_string_field(field)
        self.query_builder.add_field_match(field, value, is_exact)
        return self

    def add_field_not_match(
        self, field: SearchField, value: str, is_exact: bool = True
    ) -> "ElasticDocumentQuery":
        self.query_builder.add_field_not_match(field, value, is_exact)
        return self

    def add_range(
        self,
        field: SearchField,
        min_value: Optional[str] = None,
        max_value: Optional[str] = None,
        include_min: bool = True,
        include_max: bool = True,
    ) -> "ElasticDocumentQuery":
        field_name: str = field.field_name  # type: ignore
        self.query_builder.add_range(
            field_name, min_value, max_value, include_min, include_max
        )
        return self

    def add_wildcard(self, field: SearchField, pattern: str) -> "ElasticDocumentQuery":
        field_name: str = field.field_name  # type: ignore
        self.query_builder.add_wildcard(field_name, pattern)
        return self

    def add_fuzzy(
        self, field: SearchField, value: str, fuzziness: int = 2
    ) -> "ElasticDocumentQuery":
        field_name: str = field.field_name  # type: ignore
        self.query_builder.add_fuzzy(field_name, value, fuzziness)
        return self

    def add_boost(
        self, field: SearchField, value: str, boost: float
    ) -> "ElasticDocumentQuery":
        self.query_builder.add_boost(field.field_name, value, boost)
        return self

    def group(self, operator: LogicalOperator) -> QueryGroup:
        return self.query_builder.group(operator)

    def build(self) -> str:
        return self.query_builder.build()
