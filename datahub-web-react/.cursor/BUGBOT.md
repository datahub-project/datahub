# datahub-web-react Bugbot rules

If browser code calls `crypto.randomUUID()` directly, then:
- High: secure-context only. Use `mintRequestId` / shared agent chat utils.

If UI code imports `REDESIGN_COLORS`, `ANTD_GRAY`, or raw alchemy `colors.gray[N]`
instead of semantic theme tokens, then:
- Non-blocking style note pointing at `conf/theme/colorThemes/types.ts`.

If alchemy `<Text>` / `<Icon>` is given explicit `color` / `colorLevel` props, then:
- Non-blocking: prefer inheriting from themed parent styled-components.

If a component is gated by a feature flag / agent flag, then:
- Check the disabled/empty path still renders correctly (layout, focusable
  controls, card styling) — regressions often hide behind the flag-off branch.

If SaaS-only JSX or logic is added to an OSS React file (not `*.acryl.*` /
`*.saas.*` / `acryl/` subfolder), then:
- Flag Medium licensing/separation risk. Move SaaS-only UI into the Acryl path.
