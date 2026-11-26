# metadata-service Bugbot rules

If a PR adds a new `@ConfigurationProperties` field, env-backed setting, or other
config property that can surface via system info, then:

- Flag missing classification in
  `metadata-io/.../PropertiesCollectorConfigurationTest` (sensitive vs
  non-sensitive lists / templates).
- For secrets: also verify `PropertiesCollector` redacts the key (match an
  existing `SENSITIVE_PATTERNS` keyword or extend those patterns). The test lists
  do not drive runtime redaction by themselves.
- High security when unclassified or un-redacted secrets may leak via system-info
  APIs.
- Title: "Classify new config property"
