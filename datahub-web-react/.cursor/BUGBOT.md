# datahub-web-react Bugbot rules

UUID secure-context + semantic color-token guidance lives in root
`.cursor/BUGBOT.md`. Keep React-specific checks here only.

If alchemy `<Text>` / `<Icon>` is given explicit `color` / `colorLevel` props, then:

- Non-blocking: prefer inheriting from themed parent styled-components.

If a component is gated by a feature flag / agent flag, then:

- Check the disabled/empty path still renders correctly (layout, focusable
  controls, card styling) — regressions often hide behind the flag-off branch.

If Apollo cache updaters are edited, then:

- Flag selection-set drift / missing fields (Medium).

If schema/version UI merges sibling schemas or asset versions into one control
(e.g. schema blame / version diff / sibling dropdown that assumes identical
field sets), then:

- Flag hard assumptions that siblings/versions share the same fields, types, or
  nullability — call out the specific metadata that must stay sibling-specific.

If a connection/integration settings form saves secret fields, then:

- High when masked/obfuscated values from a read query are written back unchanged
  without an unchanged-secret sentinel or omit-if-masked merge.
- Bad: treating `****` / `4a****556` display strings as real credentials.
  (Root also covers GraphQL+form save flows; prefer one comment per finding.)

If SaaS-only JSX or logic is added to an OSS React file (not `*.acryl.*` /
`*.saas.*` / `acryl/` subfolder), then:

- Flag Medium licensing/separation risk. Move SaaS-only UI into the Acryl path.
