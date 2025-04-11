# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Installation

```console
yarn install
```

## Local Development

```sh
# This command starts a local development server and open up a browser window.
../gradlew yarnStart

# Every time a markdown file is changed, update the site by running this in a separate terminal.
# If you're making changes to the docusaurus config, you'll still need to restart the server.
../gradlew fastReload
```

## Build

```console
../gradlew yarnBuild
```

This command generates static content into the `dist` directory and can be served using any static contents hosting service. You can preview the built static site using `../gradlew serve`, although we're recommend using the local development instructions locally.

## Managing Content

Please use the following steps when adding/managing content for the docs site.

### Leverage Documentation Templates

- [Feature Guide Template](../docs/_feature-guide-template.md)
- [Metadata Ingestion Source Template](../metadata-ingestion/source-docs-template.md)

### Self-Hosted vs. DataHub Cloud

The docs site includes resources for both self-hosted (aka open-source) DataHub and DataHub Cloud alike.

- All Feature Guides should include the `FeatureAvailability` component within the markdown file itself
- Features only available via DataHub Cloud should have the `saasOnly` class if they are included in `sidebar.js` to display the small "cloud" icon:

```
{
  type: "doc",
  id: "path/to/document",
  className: "saasOnly",
},
```

### Sidebar Display Options

`generateDocsDir.ts` has a bunch of logic to auto-generate the docs site Sidebar; here are a few ways to manage how documents are displayed.

1. Leverage the document's H1 value

By default, the Sidebar will display the H1 value of the Markdown file, not the file name itself.

**NOTE:** `generateDocsDir.ts` will strip leading values of `DataHub ` and `About DataHub ` to minimize repetitive values of DataHub in the sidebar

2. Hard-code the section title in `generateDocsDir.ts`

Map the file to a hard-coded value in `const hardcoded_titles`

3. Assign a `title` separate from the H1 value

You can add the following details at the top of the markdown file:

```
---
title: [value to display in the sidebar]
---
```

_This will be ignored your H1 value begins with `DataHub ` or `About DataHub `_

**NOTE:** Assigning a value for `label:` in `sidebar.js` is not reliable, e.g.

```
  { // Don't do this
    label: "Usage Guide",
    type: "doc",
    id: "path/to/document",
  },
```

### Determine the Appropriate Sidebar Section

When adding a new document to the site, determine the appropriate sidebar section:

**What is DataHub?**

By the end of this section, readers should understand the core use cases that DataHub addresses, target end-users, high-level architecture, & hosting options.

**Get Started**

The goal of this section is to provide the bare-minimum steps required to:

- Get DataHub Running
- Optionally configure SSO
- Add/invite Users
- Create Polices & assign roles
- Ingest at least one source (i.e., data warehouse)
- Understand high-level options for enriching metadata

**Ingest Metadata**

This section aims to provide a deeper understanding of how ingestion works. Readers should be able to find details for ingesting from all systems, apply transformers, understand sinks, and understand key concepts of the Ingestion Framework (Sources, Sinks, Transformers, and Recipes).

**Enrich Metadata**

The purpose of this section is to provide direction on how to enrich metadata when shift-left isn’t an option.

**Act on Metadata**

This section provides concrete examples of acting on metadata changes in real-time and enabling Active Metadata workflows/practices.

**Deploy DataHub**

The purpose of this section is to provide the minimum steps required to deploy DataHub to the vendor of your choosing.

**Developer Guides**

The purpose of this section is to provide developers & technical users with concrete tutorials on how to work with the DataHub CLI & APIs.

**Feature Guides**

This section aims to provide plain-language feature overviews for both technical and non-technical readers alike.

## Docs Generation Features

**Includes all markdown files**

By default, all markdown files in the repository will be included in the docs site.
However, you can exclude files by modifying the `filter_patterns` array in `generateDocsDir.ts`.

Any file that is included in our docs site should be linked to from the sidebar.
You can suppress this check by adding the path to the file in a comment in `sidebar.js`:

**Inline Code Snippets**

Use an "inline" directive to include code snippets from other files. The `show_path_as_comment` option will include the path to the file as a comment at the top of the snippet.

    ```python
    {{ inline /metadata-ingestion/examples/library/data_quality_mcpw_rest.py show_path_as_comment }}
    ```

**Command Output**

Use the `{{ command-output cmd }}` directive to run subprocesses and inject the outputs into the final markdown.

    {{ command-output python -c 'print("Hello world")' }}

This also works for multi-line scripts.

    {{ command-output
    source metadata-ingestion/venv/bin/activate
    python -m <something>
    }}

Regardless of the location of the markdown file, the subcommands will be executed with working directory set to the repo root.

Only the stdout of the subprocess will be outputted. The stderr, if any, will be included as a comment in the markdown.

## Docs site generation process

This process is orchestrated by a combination of Gradle and Yarn tasks. The main entrypoint is via the `docs-website:yarnGenerate` task, which in turn eventually runs `yarn run generate`.

Steps:

1. Generate the GraphQL combined schema using the gradle's `docs-website:generateGraphQLSchema` task. This generates `./graphql/combined.graphql`.
2. Generate docs for ingestion sources using the `:metadata-ingestion:docGen` gradle task.
3. Generate docs for our metadata model using the `:metadata-ingestion:modelDocGen` gradle task.
4. Run `yarn run _generate-graphql` to produce some markdown in the `./docs` directory.
5. Run `yarn run _generate-python-sdk` to generate the Python SDK reference docs in the `./docs` directory.
6. Run the `generateDocsDir.ts` script to add markdown files from elsewhere in our repo to the `./docs` directory.
7. Run a copy or rsync to copy the `./docs` directory to `./genDocs`, and delete the `./docs` directory.
8. The docusaurus build process will then use the `./genDocs` directory as the source for the docs site.
