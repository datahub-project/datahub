## post-workflow-actions.yml Validation

:x: **Missing** — have push/PR triggers but are NOT listed:

- `Lint actions`

:x: **Extra** — listed but have no push/PR trigger (stale or renamed?):

- `ABC`

> To fix: update the `workflows:` list in `post-workflow-actions.yml`.