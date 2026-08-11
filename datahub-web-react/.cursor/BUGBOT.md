# datahub-web-react Bugbot rules

If `src/**` introduces `crypto.randomUUID(`, then:
- High: secure-context only. Use `mintRequestId` / shared agent chat utils.

If UI code imports `REDESIGN_COLORS`, `ANTD_GRAY`, or raw alchemy `colors.gray[N]`
instead of semantic theme tokens, then:
- Non-blocking style note pointing at `conf/theme/colorThemes/types.ts`.

If alchemy `<Text>` / `<Icon>` is given explicit `color` / `colorLevel` props, then:
- Non-blocking: prefer inheriting from themed parent styled-components.

If a component is gated by a feature flag / agent flag, then:
- Check the disabled/empty path still renders correctly (layout, focusable
  controls, card styling) — regressions often hide behind the flag-off branch.

If default settings/search routes change, then:
- Flag feature-flagged or privilege-gated landings (blank page risk).

If Apollo cache updaters are edited, then:
- Flag selection-set drift / missing fields (Medium).

If sibling schemas/versions are merged in one control, then:
- Flag hard assumptions that siblings are identical.

If a connection/integration settings form saves secret fields, then:
- High when masked/obfuscated values from a read query are written back unchanged
  without an unchanged-secret sentinel or omit-if-masked merge.
- Bad: treating `****` / `4a****556` display strings as real credentials.

If SaaS-only JSX or logic is added to an OSS React file (not `*.acryl.*` /
`*.saas.*` / `acryl/` subfolder), then:
- Flag Medium licensing/separation risk. Move SaaS-only UI into the Acryl path.
