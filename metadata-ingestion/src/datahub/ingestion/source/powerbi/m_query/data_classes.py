from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Optional

from lark import Tree


class AbstractIdentifierAccessor(ABC):  # To pass lint
    pass


# @dataclass
# class ItemSelector:
#     items: Dict[str, Any]
#     next: Optional[AbstractIdentifierAccessor]


@dataclass
class IdentifierAccessor(AbstractIdentifierAccessor):
    """
    statement
        public_order_date = Source{[Schema="public",Item="order_date"]}[Data]
    will be converted to IdentifierAccessor instance
    where:

        "Source" is identifier

        "[Schema="public",Item="order_date"]" is "items" in ItemSelector. Data of items varies as per DataSource

        "public_order_date" is in "next" of ItemSelector. The "next" will be None if this identifier is leaf i.e. table

    """

    identifier: str
    items: Dict[str, Any]
    next: Optional[AbstractIdentifierAccessor]


@dataclass
class DataAccessFunctionDetail:
    arg_list: Tree
    data_access_function_name: str
    identifier_accessor: Optional[IdentifierAccessor]
