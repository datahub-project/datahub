# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.
It also contains our style guide, which can be consumed by engineers.

## Style Guide

### File Structure

- What should our top-level folders be?
    - `app`: components that implement the entire application
        - Based on nav page
        - Each new high-level page gets a new top-level folder
        - Nested pages are within the same top-level folder
    - `graphql`: graphql files and generated types
    - `images`: all custom images
    - `conf` / `fonts` / `providers` / `utils`: do we need these?
    - When do we create a new one?
- What to put in `index.ts(x)`?
    - Do not create these files
- Where to put tests?
    - In `__tests__/` in the same directory as the file
    - Named `OriginalFile.test.ts(x)`
- Where to put utils?
    - For utils used in a single file: `SourceFile.utils.ts`
    - For utils used in a folder: `folderName/folderName.utils.ts`
- Where to put hooks?
    - For helper hooks used in a single file: `SourceFile.hooks.ts(x)`
    - For standalone, reusable hooks: `hookName.ts(x)`
    - Try not to write hooks that generate JSX, but sometimes it may make sense
- Where to put types?
    - Types only used once should be in the same file where they’re used
    - Shared types go in `folderName/folderName.types.ts`
- Where to put helper components?
    - If the component is only used once and is small: `parent/SourceFile.components.tsx`
        - e.g. `search/SearchPage.components.tsx`
    - If a component is shared / large and doesn’t have its own children: `parent/NewComponent.tsx`
        - e.g. `search/SearchPageHeader.tsx`
    - If the component has its own child components, put it in a new folder:`parent/NewComponent/NewComponent.tsx`
        - e.g. `search/SearchCard/SearchCard.tsx`

### Component Library

- What is an DataHub component? (previously: alchemy component)
    - Can be reused anywhere in the app
    - Generalized
        - Doesn’t depend on our aspect / data structure
        - Doesn’t depend on graphql types
- Where do reusable non-DataHub components go?
    - e.g. `EntityHealth` icon, `HoverEntityTooltip`
        - Can be reused anywhere within the web app
        - Not general: can take in very DataHub/gms specific inputs, e.g. graphql types
    - shared/
        - components/
            - entity/
                - `entity.types.ts`
                - health/
                    - `EntityHealth`
                - tooltip
                    - `HoverEntityTooltip`
            - button/
                - `DownloadSearchResultsButton`
        - hooks/
        - utils/

### Code Structure

- When to break into a new component?
    - _We should err on the side of more and smaller components (files)_
    - When JSX is getting too large or complex
    - Any logical or reusable chunk
- When to break into a hook?
    - When a component’s logic is getting too large or complex
    - Any logical or reusable chunk of logic
- When to use a React context vs passing props?
    - _Err on the side of not creating contexts — only add when necessary_
    - Globally: use a context when we want values available globally, e.g. `AppContext` or `UserContext`
    - Low-level, small-scope: use a tightly-defined context for a small, self-contained set of files
        - `folder/<Folder>Context.ts`
            - Contains type, default value, helper hooks
        - `folder/<Folder>ContextProvider.tsx`
            - Contains just the provider
    - TBD: How to handle contexts for entity components?
        - `shared/entity/EntityContext.tsx`?
- How to pass props?
    - For small amount of props, can use individual values
    - When props gets larger, try to break into logical pieces
        - e.g. `entityData` rather than individual props for each aspect of the entity
    - For components reused between OSS and SaaS:
        - SaaS-only props should go in a single field: `acrylProps: { ... }`
- Component file sections:
    - Imports
    - Constants (in SNAKE_CASE)
    - Styled components
    - Props
    - Main component

### Code Conventions

- How to pass styles to a component?
    - Use styled components for any simple components
    - For custom components (e.g. our DataHub components), take `className` in as a prop and pass it to the top-level element — whatever it makes sense for custom styles to be applied to
        - This allows us to pass styles by styled component inheritance
            - e.g. if there’s a custom component `Button`, this allows us to define `const CustomButton = styled(Button)`
    - If there are multiple elements for which custom styles can be applied, non-top-level ones should be passed as a CSSProperties prop, with a specific name, e.g. `textStyle` or `buttonStyle`
- How to handle images and icons
    - For component library icons, use `<Icon>` DataHub component and specify color and size via props
        - If size is not known, use `size="inherit"` and set `font-size` in parent div
        - Always use phosphor icons — ant and material ui icons are deprecated
    - For custom images, use `<Image>` (once it’s built) and specify color and size via props
        - For svgs, in svg definition, set `fill="currentColor"` and then … TODO
        - Alt text should be a required field
- How to do theming?
    - Use `styled-components` theming, with stringed css rather than object css

```jsx
// YES
styled.div`
    border-radius: 2px;
`;

// NO
styled.div({ borderRadius: '2px' });
```

### Code Style

_Mostly handled by linter and formatter (prettier). Mostly unimportant changes that we should be consistent on to improve readability._

- Functions vs lambdas
    - Top-level, prefer named functions (`export default function f() { }`)
    - Nested, use lambdas (`const onClick = () => { }`)
- Prefer `type` over `interface` (except when using classes)
- When to omit types
    - For variables, do not need to specify
    - For function signatures, try to specify, unless it’s too difficult. In that case, prefer inferred typing over `any`
    - Prefer a mapper function over unsafe type casting
        - If you must cast types, do so as early (high) as possible
- Prefer destructuring over dot notation, but use your own intuition
    - When accessing arrays, always option chain (i.e. `x?.[0]`)
- When to optional chain
    - If it type checks, it should be fine, except for the array access case above
- Prefer direct imports, e.g. `import React, { useState } from 'react'` over `React.useState`

## Development Commands

### Setup and Dependencies

```bash
# Install dependencies and set up dev environment
# Update dependencies after changes to package.json
../gradlew yarnInstall
```

### Running the Service

```bash
# Local development on localhost:3000 (with hot reload)
../gradlew yarnServe

# Development with remote GMS (e.g., dev01)
../gradlew yarnPreview -Pproxy="<remote-instance-url>"
# e.g. ../gradlew yarnPreview -Pproxy="https://dev01.acryl.io/"
```

### Testing and Code Quality

Run formatting / linting / type checking / relevant tests after all changes

```bash
# Generate ts files based on graphql
yarn generate

# Run formatting
yarn format

# Run linting
yarn lint

# Run type checking
yarn type-check

# Run tests
yarn test
```
