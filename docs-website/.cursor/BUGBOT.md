# docs-website Bugbot rules

If `sidebars.js` is edited, then:

- When a hand-authored doc under `docs/` (not `docs/generated/`) was added in the
  same PR, flag a missing sidebar entry (Medium).
- When a DataHub Cloud release note under `docs/managed-datahub/release-notes/`
  is added, it must be the **first** entry under "DataHub Cloud Release History"
  (newer releases go at the top).
