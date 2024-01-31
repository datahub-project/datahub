from sqlglot.dialects.tsql import TSQL

class TSQLv2(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        IDENTIFIERS = ["'", '"', ("[", "]")]