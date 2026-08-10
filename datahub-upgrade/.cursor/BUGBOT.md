# datahub-upgrade Bugbot rules

If an UpgradeStep deletes ES docs or mutates entities, then:
- Require completion recording so skip() is true after success.
- Consider idempotency under at-most-once MCL delete semantics.
