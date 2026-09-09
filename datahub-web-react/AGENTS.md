# DataHub Web React — Agent Development Guide

Canonical guide for `datahub-web-react/`. Repo-wide rules (Gradle-based formatting/linting,
commit and PR conventions, testing principles, running DataHub via `scripts/dev/datahub-dev.sh`)
live in the root [`AGENTS.md`](../AGENTS.md) and are not repeated here.

Related references:

- React-specific code review rules: [`.cursor/BUGBOT.md`](.cursor/BUGBOT.md)
- Playwright UI E2E tests: [`../e2e-test/ui/playwright/README.md`](../e2e-test/ui/playwright/README.md)

## Development Commands

From the **repo root**:

```bash
# Hot-reload dev server (preferred)
scripts/dev/datahub-dev.sh setup frontend   # one-time setup
scripts/dev/datahub-dev.sh frontend         # dev server with hot reload
```

From **`datahub-web-react/`**:

```bash
# Dev server proxying to a remote GMS instead of a local one
../gradlew yarnPreview -Pproxy="<remote-instance-url>"

# Regenerate TypeScript types after changing .graphql files
yarn generate
```

### Lint, type-check, and test (before commit / PR)

From **`datahub-web-react/`**:

```bash
# Full lint (eslint + prettier + type-check)
../gradlew :datahub-web-react:yarnLint

# Lint-fix a single file (fast; skips full type-check — run yarnLint before commit)
../gradlew -x yarnInstall -x yarnGenerate yarnLintFix -Pfile=src/path/to/file.tsx

# Vitest unit tests (icon stubs needed once per clone)
node scripts/generate-lazy-icon-stubs.js
yarn test src/path/to/file.test.tsx --run
```

`yarn type-check` in CI runs repo-wide and will surface pre-existing errors in unrelated
files. Focus on errors in files you touched — optional prop calls (`prop?.(arg)`) and
import aliases are common culprits.

## File Structure

- Top-level folders: `app` (application components, one folder per top-level nav page;
  nested pages live inside their parent's folder), `graphql` (GraphQL files and generated
  types), `images` (custom images), plus `conf`, `fonts`, `providers`, `utils`.
- Do not create `index.ts(x)` barrel files.
- Tests: in `__tests__/` next to the file under test, named `OriginalFile.test.ts(x)`.
- Utils: used by one file → `SourceFile.utils.ts`; used across a folder →
  `folderName/folderName.utils.ts`.
- Hooks: helper hooks for one file → `SourceFile.hooks.ts(x)`; standalone reusable hooks →
  `hookName.ts(x)`. Avoid hooks that return JSX unless it clearly makes sense.
- Types: used once → same file as usage; shared → `folderName/folderName.types.ts`.
- Helper components: small and single-use → `parent/SourceFile.components.tsx`; shared or
  large without child components → `parent/NewComponent.tsx`; has its own child
  components → `parent/NewComponent/NewComponent.tsx`.

## Components

- Prefer alchemy (`@components`) over `antd` — ESLint `rulesdir/no-antd-imports` enforces
  this. Files that already imported antd on the PR base (`origin/master`) are grandfathered.
  The alchemy library is documented in
  [`src/alchemy-components/README.mdx`](src/alchemy-components/README.mdx); preview
  components with `yarn storybook`.
- An alchemy component is generalized and reusable anywhere: it must not depend on DataHub
  aspects, data structures, or GraphQL types.
- Reusable DataHub-specific components (which may take GraphQL types as props) go under
  `src/app/sharedV2/` (or legacy `src/app/shared/` — follow the location of the code you
  are extending).
- Err on the side of more, smaller components: break out a new component when JSX gets
  large or complex, or for any logical/reusable chunk. Same rule for extracting hooks from
  component logic.
- Contexts: err on the side of not creating them. Use a global context only for truly
  global values (e.g. `AppContext`, `UserContext`). For a small self-contained scope, use
  `folder/<Folder>Context.ts` (type, default value, helper hooks) plus
  `folder/<Folder>ContextProvider.tsx` (just the provider).
- Props: individual values for a small number of props; group into logical objects as they
  grow (e.g. `entityData` rather than one prop per aspect). For components shared between
  OSS and SaaS, SaaS-only props go in a single `acrylProps: { ... }` field.
- Component file section order: imports, constants (SNAKE_CASE), styled components, props,
  main component.

## Styling, Theming, Icons, and Images

- Use `styled-components` with string CSS, not object CSS:

    ```tsx
    // YES
    styled.div`
        border-radius: 2px;
    `;

    // NO
    styled.div({ borderRadius: '2px' });
    ```

- Custom components should accept `className` and pass it to the element custom styles
  should apply to — this enables `const CustomButton = styled(Button)` inheritance. If
  multiple inner elements are styleable, expose non-top-level ones as named
  `CSSProperties` props (e.g. `textStyle`, `buttonStyle`).

**Colors — always use semantic color tokens** from `src/conf/theme/colorThemes/types.ts`.
Never use hardcoded hex values, `REDESIGN_COLORS`, `ANTD_GRAY`, or direct alchemy
`colors.gray[X]` imports.

In styled-components (no import needed — `theme` is available via props):

```typescript
background: ${(props) => props.theme.colors.bg};
color: ${(props) => props.theme.colors.text};
border: 1px solid ${(props) => props.theme.colors.border};
```

In React component bodies:

```typescript
import { useTheme } from 'styled-components';
const theme = useTheme();
<Icon color={theme.colors.icon} />
```

For alchemy components (`<Text>`, `<Icon>`, etc.) — do not pass `color`/`colorLevel`
props; let them inherit from themed parent styled-components.

Do not import from:

- `src/alchemy-components/theme/foundations/colors.ts` (raw palette, only used internally
  by the theme)
- `REDESIGN_COLORS` or `ANTD_GRAY` from `entityV2/shared/constants.ts`

**Icons**: use the alchemy `<Icon>` component with phosphor icons only — ant and material
UI icons are deprecated. Specify color and size via props; if size is unknown, use
`size="inherit"` and set `font-size` in the parent element.

**Images**: use the alchemy `<LoadedImage>` component (`src` and `alt` are required; it
handles loading skeleton and error fallback). For SVGs, set `fill="currentColor"` in the
SVG definition so the icon inherits color from CSS.

## TypeScript Style

Most style is enforced by ESLint/Prettier. Conventions the tools don't enforce:

- Prefer `type` over `interface` (except when using classes). Type props with TypeScript
  types, never PropTypes.
- Top-level: named functions (`export default function f() {}`); nested: lambdas
  (`const onClick = () => {}`).
- Don't annotate variables; do annotate function signatures unless impractical — prefer
  inferred typing over `any`. Prefer a mapper function over unsafe casting; if you must
  cast, do it as early (high) as possible.
- Always optional-chain array access (`x?.[0]`).
- Prefer direct imports (`import React, { useState } from 'react'`) over `React.useState`.

## Unit Testing

- Vitest + React Testing Library; tests live in `__tests__/` next to the source file.
- Wrap components in `TestPageContainer` from `@utils/test-utils/TestPageContainer` — it
  provides the necessary app providers (theme, routing, contexts).
- Use `MockedProvider` from `@apollo/client/testing` for components that make GraphQL
  queries.
- Generate icon stubs once per clone before running tests:
  `node scripts/generate-lazy-icon-stubs.js`.
- Follow the root guide's testing principles: test behavior, not implementation; skip
  tests that only exercise the framework or restate the linter.
