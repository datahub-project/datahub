# metadata-io Bugbot rules

Classification + redaction for new config properties is in the root
`.cursor/BUGBOT.md` (config often lands under `metadata-service/`). Keep
metadata-io-specific notes here only.

If this module changes `PropertiesCollector` redaction patterns or
`PropertiesCollectorConfigurationTest` lists, then:

- Verify sensitive keys are both classified **and** actually redacted at runtime
  (test list alone does not redact — keys must match `SENSITIVE_PATTERNS` or an
  extended pattern).
- Title: "Config redaction must match classification"
