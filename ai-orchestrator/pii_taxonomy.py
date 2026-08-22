"""The label set written to DataHub, and the provenance tag that marks machine writes.

One label per column. The set is deliberately narrow and maps to categories a privacy
team already reasons about (DSAR scope, retention, consent), because a label nobody can
act on is worse than no label.
"""
from __future__ import annotations

from dataclasses import dataclass


PROVENANCE_TAG = "AI-Proposed"

# Below this, a verdict is surfaced as "needs a judgement call" and is not written unless
# the reviewer explicitly opts in.
DEFAULT_CONFIDENCE_FLOOR = 0.6


@dataclass(frozen=True, slots=True)
class PiiLabel:
    name: str
    description: str
    # Sent to the model for the residual pass only. Kept short: this text is the whole
    # definition the model gets, so it has to be decidable from a column name and type.
    guidance: str


LABELS: tuple[PiiLabel, ...] = (
    PiiLabel(
        "PII.Name",
        "A natural person's name.",
        "A person's name or part of one. Not the name of a table, product, or company.",
    ),
    PiiLabel(
        "PII.Email",
        "An email address belonging to a person.",
        "An email address. A boolean about an address is not the address.",
    ),
    PiiLabel(
        "PII.Phone",
        "A telephone or mobile number.",
        "A phone, mobile, or fax number.",
    ),
    PiiLabel(
        "PII.Address",
        "A postal address, or a component precise enough to narrow location.",
        "Street, city, postcode, or full postal address. Country alone is too coarse.",
    ),
    PiiLabel(
        "PII.Geolocation",
        "Coordinates that locate a person precisely.",
        "Latitude, longitude, or GPS coordinates.",
    ),
    PiiLabel(
        "PII.DateOfBirth",
        "A date of birth, or an age derived from one.",
        "Date of birth or age. Not an account creation or event date.",
    ),
    PiiLabel(
        "PII.NationalID",
        "A government-issued identifier.",
        "Government identifier: SSN, passport, tax ID, national ID, driving licence.",
    ),
    PiiLabel(
        "PII.FinancialAccount",
        "A payment instrument or bank account identifier.",
        "Card number, IBAN, bank account, routing number, CVV. Not a monetary amount.",
    ),
    PiiLabel(
        "PII.Credentials",
        "A secret that authenticates a person or session.",
        "Password (even hashed), secret, API key, access or refresh token, MFA seed.",
    ),
    PiiLabel(
        "PII.Health",
        "Health, medical, or disability information.",
        "Diagnosis, treatment, prescription, blood type, disability, or other health data.",
    ),
    PiiLabel(
        "PII.Demographic",
        "A special-category attribute such as gender, race, religion, or marital status.",
        "Gender, race, ethnicity, religion, nationality, citizenship, marital status.",
    ),
    PiiLabel(
        "PII.Compensation",
        "Pay, salary, or equity for an identifiable person.",
        "Salary, wage, bonus, pay rate, or equity grant. Not a generic transaction amount.",
    ),
    PiiLabel(
        "PII.DeviceID",
        "A device, cookie, or advertising identifier that tracks a person.",
        "Device ID, IMEI, MAC, advertising ID, cookie ID, or browser fingerprint.",
    ),
    PiiLabel(
        "PII.IPAddress",
        "An IP address, treated as personal data under GDPR.",
        "An IPv4 or IPv6 address.",
    ),
    PiiLabel(
        "PII.UserID",
        "A pseudonymous identifier scoped to one person.",
        "An identifier for a person (user, customer, employee, patient). Not a surrogate "
        "key for a non-person row such as an order or invoice.",
    ),
)

BY_NAME: dict[str, PiiLabel] = {label.name: label for label in LABELS}

ALL_TAGS: tuple[str, ...] = tuple(label.name for label in LABELS) + (PROVENANCE_TAG,)


def tag_urn(tag_name: str) -> str:
    return f"urn:li:tag:{tag_name}"


def is_taxonomy_tag(urn_or_name: str) -> bool:
    """Whether a tag is ours, so repeat runs can leave a steward's own tags untouched."""
    name = urn_or_name.rsplit(":", 1)[-1]
    return name in BY_NAME or name == PROVENANCE_TAG


def guidance_block() -> str:
    """The label menu for the residual LLM pass."""
    return "\n".join(f"- {label.name}: {label.guidance}" for label in LABELS)
