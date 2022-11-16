from abc import ABC, abstractmethod
from typing import Optional, List, Dict
import importlib.resources as pkg_resource
from lark import Lark


class Token(ABC):
    @abstractmethod
    def parse_raw_token(self) -> str:
        pass


class BaseToken(Token, ABC):
    _raw_token: str
    _nested_tokens: Optional[List["BaseToken"]]

    def __init__(self, raw_token: str, nested_tokens: Optional[List["BaseToken"]]):
        self._raw_token = raw_token
        self._nested_tokens = nested_tokens
        self.parse_raw_token(self._raw_token)


class LetToken(BaseToken):
    def __init__(self, raw_token: str, nested_raw_tokens: Optional[List["Token"]]):
        super().__init__(raw_token, nested_raw_tokens)

    def parse_raw_token(self) -> str:
        pass


class TableFuncToken(BaseToken):
    def __init__(self, raw_token: str, nested_raw_tokens: Optional[List["BaseToken"]]):
        super().__init__(raw_token, nested_raw_tokens)

    def parse_raw_token(self) -> str:
        pass


class DataAccessToken(BaseToken):
    def __init__(self, raw_token: str, nested_raw_tokens: Optional[List["BaseToken"]]):
        super().__init__(raw_token, nested_raw_tokens)

    def parse_raw_token(self) -> str:
        pass


class OracleDataAccessToken(BaseToken):
    def __init__(self, raw_token: str, nested_raw_tokens: Optional[List["BaseToken"]]):
        super().__init__(raw_token, nested_raw_tokens)

    def parse_raw_token(self) -> str:
        pass


class Step:
    tokens: List[BaseToken]
    def __init__(self, tokens: List[BaseToken]):
        self.tokens = tokens


token_registry: Dict[str, BaseToken] = {
    "let": LetToken,
    "Table": TableFuncToken,
    "PostgreSQL.Database": DataAccessToken,
    "DB2.Database": DataAccessToken,
    "Sql.Database": DataAccessToken,
    "Oracle.Database": OracleDataAccessToken,
}


def parse_expression(expression: str) -> List[Step]:
    grammar: str = pkg_resource.read_text("datahub.ingestion.source.powerbi", "powerbi-lexical-grammar.rule")
    lark_parser = Lark(grammar,  start="let_expression", regex=True)
    print(lark_parser.parse(expression).pretty())
