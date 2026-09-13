"""Deterministic recognisers, run before the model.

Most PII columns are named exactly what they are, and a lookup decides those faster,
more cheaply, and more repeatably than a language model can. The model is reserved for
the columns that genuinely need context — `name` on `hr_employees` is a person, `name`
on `products` is not, and only one of those needs an inference.

Matching is on whole tokens, not substrings: `ip` must not fire on `description`, and
`pan` must not fire on `company`. Rule order is priority order, first match wins.
"""
from __future__ import annotations

import re

from pii_models import Column, Decision, Source, Verdict

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM = re.compile(r"[^0-9A-Za-z]+")


def normalize(field_path: str) -> str:
    """`[version=2.0].[type=struct].clientIP` -> `client_ip`.

    Only the leaf matters: a nested path's parents describe the struct, not the value.
    """
    leaf = field_path.split(".")[-1]
    leaf = _CAMEL_BOUNDARY.sub("_", leaf)
    parts = [part for part in _NON_ALNUM.split(leaf) if part]
    return "_".join(part.lower() for part in parts)


def _tok(*alternatives: str) -> re.Pattern[str]:
    """Whole-token match. Alternatives may contain `_` to match a token sequence."""
    return re.compile(rf"(?:^|_)(?:{'|'.join(alternatives)})(?:_|$)")


def _exact(*alternatives: str) -> re.Pattern[str]:
    """Whole-name match, for names too generic to match as a token.

    `id` has to be anchored: as a token it also matches the `_id` tail of every
    `*_id` column, which would quietly drop `session_id` and `account_id` as surrogate
    keys instead of letting the model judge whether they identify a person.
    """
    return re.compile(rf"^(?:{'|'.join(alternatives)})$")


class Rule:
    __slots__ = ("label", "pattern", "confidence", "reason")

    def __init__(self, label: str, pattern: re.Pattern[str], confidence: float, reason: str):
        self.label = label
        self.pattern = pattern
        self.confidence = confidence
        self.reason = reason


# A boolean or aggregate *about* a PII field is not that field. `email_verified` is a
# flag, not an address. These are demoted to the model rather than tagged or dropped,
# because `verified_email` — the address itself — matches the same guard.
_DERIVED = _tok(
    "is", "has", "any", "num", "count", "total", "flag", "enabled", "disabled",
    "verified", "unverified", "valid", "invalid", "exists", "missing", "present",
)

# `name` qualified by a non-person noun. `company_name` is not a person's name.
_NON_PERSON = (
    "table", "file", "column", "field", "product", "company", "org", "organization",
    "brand", "plan", "role", "event", "job", "service", "bank", "merchant", "vendor",
    "store", "platform", "team", "group", "dept", "department", "project", "report",
    "dashboard", "metric", "tag", "topic", "queue", "index", "schema", "db", "database",
    "host", "cluster", "region", "zone", "bucket", "app", "package", "class", "method",
)
_NON_PERSON_NAME = re.compile(
    rf"(?:^|_)(?:{'|'.join(_NON_PERSON)})_(?:name|nm|title|label)(?:_|$)"
)

# Real schemas abbreviate heavily (`given_nm`, `reporter_eml`, `emp_no`, `cust_ref`).
# These are spelled out rather than left to the model, because an inference per
# abbreviation is the difference between a sub-second proposal and a five-second one.
_PERSON_NOUNS = (
    "user", "cust", "customer", "member", "person", "people", "subscriber", "employee",
    "emp", "staff", "patient", "contact", "client", "party", "holder", "payee", "payer",
)
_PERSON_REF = re.compile(
    rf"(?:^|_)(?:{'|'.join(_PERSON_NOUNS)})_(?:ref|no|num|number|key|code)\d*(?:_|$)"
)
_NAME_ABBREV = re.compile(r"(?:^|_)(?:nm)$")

POSITIVE_RULES: tuple[Rule, ...] = (
    Rule(
        "PII.Credentials",
        _tok(
            "password", "passwd", "pwd", "password_hash", "pass_hash", "secret",
            "secret_key", "api_key", "apikey", "access_token", "refresh_token",
            "auth_token", "bearer_token", "session_token", "private_key", "mfa_secret",
            "otp_secret", "totp_secret", "security_answer",
        ),
        0.95,
        "Authentication secret; sensitive even when hashed",
    ),
    Rule(
        "PII.NationalID",
        _tok(
            "ssn", "social_security", "social_security_number", "national_id",
            "nationalid", "aadhaar", "aadhar", "passport", "passport_number",
            "passport_no", "tax_id", "taxid", "tin", "itin", "voter_id", "nin", "sin",
        ),
        0.95,
        "Government-issued identifier",
    ),
    Rule(
        "PII.NationalID",
        _tok(
            "nino", "ni_number", "ni_no", "national_insurance", "pps_number", "nif",
            "cpf", "curp", "rfc", "sin_number",
        ),
        0.93,
        "National insurance or tax identifier",
    ),
    Rule(
        "PII.NationalID",
        _tok("drivers_license", "driver_license", "driving_licence", "licence_number", "license_number"),
        0.8,
        "Looks like a driving licence, though it could be a software licence",
    ),
    Rule(
        "PII.NationalID",
        _tok("vat_id", "vat_number", "vat_no"),
        0.7,
        "VAT number, which identifies a person when the trader is an individual",
    ),
    Rule(
        "PII.FinancialAccount",
        _tok(
            "card_number", "card_no", "cardnumber", "credit_card", "creditcard",
            "debit_card", "cc_number", "ccnum", "primary_account_number", "iban", "bic",
            "swift_code", "bank_account", "routing_number", "sort_code", "cvv", "cvc",
            "card_last4", "last4", "card_last_four", "last_four", "card_expiry",
            "card_exp", "card_pan", "card_pan_masked", "pan_masked", "exp_month",
            "exp_year",
        ),
        0.95,
        "Payment instrument identifier",
    ),
    Rule(
        "PII.FinancialAccount",
        _tok("account_number", "acct_number", "acct_no"),
        0.75,
        "Account number, though it may identify a tenant rather than a person",
    ),
    Rule(
        "PII.Email",
        _tok("email", "emails", "e_mail", "email_address", "email_addr", "mail_address", "eml"),
        0.97,
        "Email address",
    ),
    Rule(
        "PII.Phone",
        _tok(
            "phone", "phones", "phone_number", "phone_no", "phone_num", "mobile",
            "mobile_number", "mobile_no", "msisdn", "telephone", "tel", "tel_no", "fax",
            "cell", "cell_phone", "contact_number", "contact_no", "callback_no",
            "whatsapp",
        ),
        0.95,
        "Telephone number",
    ),
    Rule(
        "PII.IPAddress",
        _tok(
            "ip", "ip_address", "ipaddr", "ipv4", "ipv6", "client_ip", "remote_ip",
            "remote_addr", "source_ip", "x_forwarded_for", "forwarded_for",
        ),
        0.95,
        "IP address; personal data under GDPR",
    ),
    Rule(
        "PII.DateOfBirth",
        _tok("dob", "date_of_birth", "birth_date", "birthdate", "birthday", "born_on"),
        0.96,
        "Date of birth",
    ),
    Rule(
        "PII.DateOfBirth",
        _tok("age", "birth_year"),
        0.7,
        "Derived from date of birth",
    ),
    Rule(
        "PII.Name",
        _tok(
            "first_name", "last_name", "middle_name", "full_name", "given_name",
            "family_name", "surname", "sur_name", "forename", "fore_name", "maiden_name",
            "legal_name", "preferred_name", "fname", "lname", "mname", "person_name",
            "employee_name", "customer_name", "contact_name", "patient_name",
            "given_nm", "sur_nm", "first_nm", "last_nm", "full_nm", "middle_nm",
            "legal_nm", "reporter_nm", "cardholder", "card_holder", "acct_holder",
            "account_holder",
        ),
        0.95,
        "Person's name",
    ),
    Rule(
        "PII.Name",
        _tok("display_name", "nickname", "nick_name", "screen_name", "alias"),
        0.75,
        "Display name, which usually contains or reveals a person's name",
    ),
    Rule(
        "PII.Address",
        _tok(
            "address", "addr", "street", "street_address", "address_line1",
            "address_line2", "addr_line1", "addr_line2", "postal", "postal_code",
            "postcode", "post_code", "postal_cd", "zip", "zip_code", "zipcode",
            "pin_code", "house_number", "apartment", "apt", "po_box",
        ),
        0.9,
        "Postal address component; narrows location to a small area",
    ),
    Rule(
        "PII.Address",
        _tok("city", "town", "district", "county", "suburb"),
        0.7,
        "Coarse location, identifying in combination with other columns",
    ),
    Rule(
        "PII.Geolocation",
        _tok(
            "latitude", "longitude", "lat", "lon", "lng", "geo_lat", "geo_lon", "gps",
            "gps_coordinates", "coordinates", "geo_point",
        ),
        0.9,
        "Precise coordinates",
    ),
    Rule(
        "PII.DeviceID",
        _tok(
            "device_id", "deviceid", "device_token", "device_fingerprint",
            "advertising_id", "ad_id", "idfa", "idfv", "gaid", "aaid", "imei", "imsi",
            "udid", "mac_address", "mac_addr", "cookie_id", "browser_fingerprint",
            "fingerprint", "user_agent", "useragent",
        ),
        0.85,
        "Device or browser identifier used to track a person",
    ),
    Rule(
        "PII.Health",
        _tok(
            "diagnosis", "diagnoses", "icd", "icd10", "icd_code", "medical",
            "medical_record", "mrn", "health", "health_condition", "blood_type",
            "blood_group", "allergy", "allergies", "prescription", "medication",
            "treatment", "disability", "disability_status", "symptoms",
        ),
        0.9,
        "Health information",
    ),
    Rule(
        "PII.Demographic",
        _tok(
            "gender", "sex", "race", "ethnicity", "ethnic_group", "religion",
            "marital_status", "nationality", "citizenship", "veteran_status",
            "sexual_orientation", "political_affiliation", "union_membership",
        ),
        0.85,
        "Special-category demographic attribute",
    ),
    Rule(
        "PII.Compensation",
        _tok(
            "salary", "base_salary", "annual_salary", "gross_salary", "net_salary",
            "compensation", "total_comp", "ctc", "wage", "hourly_rate", "pay_rate",
            "pay_grade", "bonus", "equity", "stock_grant", "rsu", "gross_annual",
            "gross_pay", "net_pay", "annual_pay", "take_home", "payroll_amount",
        ),
        0.85,
        "Compensation for an identifiable person",
    ),
    Rule(
        "PII.UserID",
        _tok(
            "user_id", "userid", "customer_id", "cust_id", "employee_id", "emp_id",
            "member_id", "person_id", "patient_id", "subscriber_id", "contact_id",
            "corp_user", "corpuser", "username", "user_name", "login", "login_id",
            "created_by", "updated_by", "owner_id", "actor_id", "requester_id",
            "assignee_id",
        ),
        0.7,
        "Pseudonymous identifier scoped to one person",
    ),
    Rule(
        "PII.UserID",
        _PERSON_REF,
        0.7,
        "Person-scoped reference number, pseudonymous but still personal data",
    ),
    Rule(
        "PII.Name",
        _NAME_ABBREV,
        0.7,
        "`_nm` is a name abbreviation; person unless the qualifier says otherwise",
    ),
)

# Only what cannot describe a person. Recall matters more than saving a model call, so
# anything arguable is left to the model instead of being dropped here.
NEGATIVE_RULES: tuple[re.Pattern[str], ...] = (
    # Any `*_at` / `*_date` / `*_time` tail is temporal. Birth dates are unaffected
    # because the positive rules run first and claim them.
    re.compile(r"(?:^|_)(?:at|on|date|time|ts|timestamp)$"),
    _exact("date", "time", "timestamp", "datetime"),
    _tok("timestamp", "expiry", "duration", "elapsed", "latency", "ttl"),
    # Employment and org attributes describe a role, not a person.
    _tok(
        "department", "dept", "division", "job_title", "designation", "grade", "level",
        "seniority", "employment_type", "cost_center", "business_unit",
    ),
    # Deliberately not `name`, `data`, or `payload`: those depend on the table, so they
    # go to the model rather than being dropped here.
    _exact("id", "pk", "sk", "key"),
    _tok(
        "uuid", "guid", "row_id", "order_id", "invoice_id",
        "transaction_id", "txn_id", "payment_id", "product_id", "item_id", "sku",
        "event_id", "request_id", "trace_id", "span_id", "job_id", "run_id", "batch_id",
        "tenant_id", "org_id", "company_id", "team_id", "group_id", "role_id",
        "policy_id", "message_id", "ticket_id", "correlation_id", "parent_id",
    ),
    _tok(
        "count", "total", "total_amount", "amount", "sum", "avg", "min", "max", "qty",
        "quantity", "price", "unit_price", "cost", "balance", "currency",
        "currency_code", "version", "app_version", "revision", "seq", "sequence",
        "offset", "limit", "page", "size", "length", "status", "state", "type", "kind",
        "category", "enabled", "disabled", "active", "deleted", "score", "rank",
        "flag", "default", "resolved", "affected", "rows_affected", "lifetime_value",
        "ltv", "revenue", "spend", "usd", "brand", "card_brand", "card_type",
        "card_network", "log_id", "merchant_id", "target_table",
    ),
    _tok(
        "schema", "database", "catalog", "partition", "platform", "environment", "env",
        "source", "source_system", "locale", "language", "lang", "timezone", "tz",
        "country", "country_code", "url", "uri", "endpoint", "method", "http_method",
        "status_code", "error_code", "error_message", "action", "operation", "channel",
        "description", "notes", "comment", "reason",
    ),
    _NON_PERSON_NAME,
)


def _match(norm: str) -> Rule | None:
    for rule in POSITIVE_RULES:
        if rule.pattern.search(norm):
            return rule
    return None


def _is_negative(norm: str) -> bool:
    return any(pattern.search(norm) for pattern in NEGATIVE_RULES)


def apply_rules(columns: list[Column]) -> Decision:
    """Split columns into settled verdicts and a residual for the model."""
    verdicts: list[Verdict] = []
    residual: list[Column] = []

    for column in columns:
        norm = normalize(column.field_path)
        rule = _match(norm)

        # A qualified `name` is never a person, whatever else matched.
        if rule is not None and rule.label == "PII.Name" and _NON_PERSON_NAME.search(norm):
            continue

        if rule is not None and _DERIVED.search(norm):
            residual.append(column)
            continue

        if rule is not None:
            verdicts.append(
                Verdict(
                    field=column.field_path,
                    label=rule.label,
                    confidence=rule.confidence,
                    reason=rule.reason,
                    source=Source.RULE,
                )
            )
            continue

        if _is_negative(norm):
            continue

        residual.append(column)

    return Decision(verdicts=verdicts, residual=residual)
