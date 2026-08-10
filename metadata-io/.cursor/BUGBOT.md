# metadata-io Bugbot rules

If a PR adds a new `@ConfigurationProperties` field, env-backed setting, or other
config property that can surface via system info, then:
- Flag missing classification in
  `PropertiesCollectorConfigurationTest` (sensitive vs non-sensitive lists).
- High security: unclassified properties may leak secrets in system-info APIs.
- Title: "Classify new config property"
