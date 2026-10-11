# docs-website — Agent Development Guide

The docs site is a **Docusaurus 2** app. Docusaurus reads from `genDocs/` (gitignored, assembled at
build time). Custom React pages live in `src/pages/`, blog/learn articles in `src/learn/`.

## Build pipeline

The full build runs as a Gradle task chain. You rarely need to invoke individual steps, but
understanding the pipeline helps when debugging.

```
./gradlew :docs-website:yarnBuild
  └─ yarnGenerate
       ├─ yarnInstall
       ├─ generateGraphQLSchema   → graphql/combined.graphql
       ├─ generateJsonSchema      → static/schemas/datahub_ingestion_schema.json
       ├─ :metadata-ingestion:modelDocGen → docs/generated/metamodel/
       └─ :metadata-ingestion:docGen      → docs/generated/ingestion/
```

`yarnGenerate` runs `yarn run generate`, which:

1. Generates GraphQL API docs via the `docusaurus-graphql-plugin`
2. Generates Python SDK docs via Sphinx (`sphinx/` → Markdown conversion)
3. Runs `generateDocsDir.ts` to assemble all markdown into `genDocs/`

## `generateDocsDir.ts` — the assembly script

This TypeScript script is the central piece that discovers all markdown in the repo and writes
processed output to `genDocs/`. Key behaviors:

- **Discovery**: Uses `git ls-files` to find all `.md` files, then filters out excluded paths
  (`.github/`, `node_modules/`, `contrib/`, archived docs, test dirs, etc.)
- **Frontmatter injection**: Adds `title`, `slug`, `description`, `custom_edit_url` to each file
- **Link rewriting**: Rewrites relative links and image paths for the hosted site
- **Inline directives**: `{{ inline /path/to/file.py }}` embeds file contents;
  `{{ command-output cmd }}` embeds command output
- **Sidebar validation**: Warns if a doc file is not referenced in `sidebars.js` (some dirs are
  exempt: `docs/generated/metamodel`, `docs/generated/ingestion`, `docs/actions/`, etc.)

## `sidebars.js` conventions

- Doc IDs are file paths relative to `genDocs/`, minus the `.md` extension
- Every hand-authored doc in `docs/` must appear in `sidebars.js` or the build warns
- Auto-generated directories (`docs/generated/metamodel/`, `docs/generated/ingestion/`) are exempt

## Key files

| File                    | Purpose                                           |
| ----------------------- | ------------------------------------------------- |
| `docusaurus.config.js`  | Site config — plugins, navbar, footer, docs path  |
| `sidebars.js`           | Sidebar navigation tree                           |
| `generateDocsDir.ts`    | Markdown discovery, transformation, and assembly  |
| `build.gradle`          | Gradle tasks (`yarnGenerate`, `yarnBuild`, etc.)  |
| `sphinx/`               | Python SDK doc generation (Sphinx → Markdown)     |
| `graphql/`              | GraphQL schema combination + doc generation       |
| `genJsonSchema/`        | Combines connector config schemas into one file   |
| `src/pages/`            | Custom React pages (not markdown docs)            |
| `src/learn/`            | Blog / learning articles (served at `/learn`)     |
| `static/`               | Images, logos, static assets                      |

## Generated directories (gitignored — never edit)

- `genDocs/` — final assembled docs consumed by Docusaurus
- `graphql/combined.graphql` — concatenated GraphQL schema
- `static/schemas/datahub_ingestion_schema.json` — combined ingestion config schema

## Logo convention

Platform logos: `static/img/logos/platforms/{platform_id}.svg`
DataHub internal connectors: use `img/datahub-logo-color-mark.svg` (shared logo).
