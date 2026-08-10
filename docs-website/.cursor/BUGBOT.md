# docs-website Bugbot rules

If a PR adds or materially edits hand-authored docs under `docs/` (not
`docs/generated/`) without a matching entry in `docs-website/sidebars.js`, then:
- Medium: doc may publish but never appear in nav.
- Title: "Missing sidebars.js entry"

If a DataHub Cloud release note is added under
`docs/managed-datahub/release-notes/` without being the first entry under
"DataHub Cloud Release History" in `sidebars.js`, then:
- Flag: release note won't show in sidebar (newer releases go at the top).
