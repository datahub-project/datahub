from abc import ABC, abstractmethod
from typing import Optional, List, Dict


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


# identifier with space are not supported.
# This is one of the way to create identifier in M https://learn.microsoft.com/en-us/powerquery-m/expression-identifier
def parse_expression(expression: str) -> List[Step]:
    strip_expression: str = expression.strip()
    raw_token: str = ""
    index: int = 0
    for c in strip_expression:
        if c == ' ':
            continue

        raw_token = raw_token + c
