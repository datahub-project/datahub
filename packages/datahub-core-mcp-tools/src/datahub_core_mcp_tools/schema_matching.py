"""Deployment-selected column ranking, independent of the tool transport."""

from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True)
class FieldText:
    path: str
    text: tuple[str, ...]


class SchemaFieldMatcher(Protocol):
    def scores(
        self, keywords: Sequence[str], fields: Sequence[FieldText]
    ) -> list[float]:
        """Return one nonnegative score per field, in order; zero means no match.

        Scores are comparable within this call only. The full schema is supplied
        so matchers can use its column frequencies when ranking keywords.
        """
        ...


class SubstringMatcher:
    def scores(
        self, keywords: Sequence[str], fields: Sequence[FieldText]
    ) -> list[float]:
        terms = [keyword.lower() for keyword in keywords]
        return [
            float(
                sum(
                    1
                    for term in terms
                    for text in (field.path, *field.text)
                    if text and term in text.lower()
                )
            )
            for field in fields
        ]
