import urllib.parse
from typing import List

from datahub.utilities.urns.error import InvalidUrnError


def guess_entity_type(urn: str) -> str:
    assert urn.startswith("urn:li:"), "urns must start with urn:li:"
    return urn.split(":")[2]


class Urn:
    """
    URNs are Globally Unique Identifiers (GUID) used to represent an entity.
    It will be in format of urn:<domain>:<type>:<id>
    """

    URN_PREFIX: str = "urn"
    # all the Datahub urn use li domain for now.
    LI_DOMAIN: str = "li"

    _entity_type: str
    _domain: str
    _entity_id: List[str]

    def __init__(
        self, entity_type: str, entity_id: List[str], urn_domain: str = LI_DOMAIN
    ):
        if not entity_id:
            raise InvalidUrnError("Empty entity id.")
        self._validate_entity_type(entity_type)
        self._validate_entity_id(entity_id)
        self._entity_type = entity_type
        self._domain = urn_domain
        self._entity_id = entity_id

    @classmethod
    def create_from_string(cls, urn_str: str) -> "Urn":
        """
        Create a Urn from the its string representation
        :param urn_str: the string representation of the Urn
        :return: Urn of the given string representation
        :raises InvalidUrnError if the string representation is in invalid format
        """

        # expect urn string in format of urn:<domain>:<type>:<id>
        cls.validate(urn_str)
        parts: List[str] = urn_str.split(":", 3)

        return cls(parts[2], cls._get_entity_id_from_str(parts[3]), parts[1])

    @classmethod
    def validate(cls, urn_str: str) -> None:
        """
        Validate if a string is in valid Urn format
        :param urn_str: to be validated urn string
        :raises InvalidUrnError if the string representation is in invalid format
        """
        parts: List[str] = urn_str.split(":", 3)
        if len(parts) != 4:
            raise InvalidUrnError(
                f"Invalid urn string: {urn_str}. Expect 4 parts from urn string but found {len(parts)}"
            )

        if "" in parts:
            raise InvalidUrnError(
                f"Invalid urn string: {urn_str}. There should not be empty parts in urn string."
            )

        if parts[0] != Urn.URN_PREFIX:
            raise InvalidUrnError(
                f'Invalid urn string: {urn_str}. Expect urn starting with "urn" but found {parts[0]}'
            )

        if "" in cls._get_entity_id_from_str(parts[3]):
            raise InvalidUrnError(
                f"Invalid entity id in urn string: {urn_str}. There should not be empty parts in entity id."
            )

        cls._validate_entity_type(parts[2])
        cls._validate_entity_id(cls._get_entity_id_from_str(parts[3]))

    @staticmethod
    def url_encode(urn: str) -> str:
        # safe='' encodes '/' as '%2F'
        return urllib.parse.quote(urn, safe="")

    def get_type(self) -> str:
        return self._entity_type

    def get_entity_id(self) -> List[str]:
        return self._entity_id

    def get_entity_id_as_string(self) -> str:
        """
        :return: string representation of the entity ids. If there are more than one part in the entity id part, it will
        return in this format (<part1>,<part2>,...)
        """
        return self._entity_id_to_string()

    def get_domain(self) -> str:
        return self._domain

    @staticmethod
    def _get_entity_id_from_str(entity_id: str) -> List[str]:
        if not (entity_id.startswith("(") and entity_id.endswith(")")):
            return [entity_id]

        parts = []
        start_paren_count = 1
        part_start = 1
        for i in range(1, len(entity_id)):
            c = entity_id[i]
            if c == "(":
                start_paren_count += 1
            elif c == ")":
                start_paren_count -= 1
                if start_paren_count < 0:
                    raise InvalidUrnError(f"{entity_id}, mismatched paren nesting")
            elif c == ",":
                if start_paren_count != 1:
                    continue

                if i - part_start <= 0:
                    raise InvalidUrnError(f"{entity_id}, empty part disallowed")
                parts.append(entity_id[part_start:i])
                part_start = i + 1

        if start_paren_count != 0:
            raise InvalidUrnError(f"{entity_id}, mismatched paren nesting")

        parts.append(entity_id[part_start:-1])

        return parts

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        pass

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        pass

    def __str__(self) -> str:
        return f"{self.URN_PREFIX}:{self._domain}:{self._entity_type}:{self._entity_id_to_string()}"

    def _entity_id_to_string(self) -> str:
        if len(self._entity_id) == 1:
            return self._entity_id[0]
        result = ""
        for part in self._entity_id:
            result = result + str(part) + ","
        return f"({result[:-1]})"

    def __hash__(self) -> int:
        return hash((self._domain, self._entity_type) + tuple(self._entity_id))

    def __eq__(self, other: object) -> bool:
        return (
            (
                self._entity_id == other._entity_id
                and self._domain == other._domain
                and self._entity_type == other._entity_type
            )
            if isinstance(other, Urn)
            else False
        )
