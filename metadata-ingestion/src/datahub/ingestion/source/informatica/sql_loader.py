from functools import lru_cache
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"


@lru_cache(maxsize=None)
def load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL 파일을 찾을 수 없습니다: {filename}")
    return path.read_text()
