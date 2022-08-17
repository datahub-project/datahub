# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Installation

```console
yarn install
```

## Local Development

```console
../gradlew yarnStart

# You may also have success running the underlying commands manually.
yarn run lint
yarn run generate
yarn run start
```

This command starts a local development server and open up a browser window.

## Build

```console
../gradlew yarnBuild
```

This command generates static content into the `dist` directory and can be served using any static contents hosting service. You can preview the built static site using `../gradlew serve`, although we're recommend using the local development instructions locally.

## Generating GraphQL API Docs

To regenerate GraphQL API docs, simply rebuild the docs-website directory. 

```console
./gradlew docs-website:build
```