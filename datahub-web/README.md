DataHub Web Client
==============================================================================

## About
This mono-repository is for the portal web-client and related packages for DataHub, LinkedIn's premier
data search and discovery tool, connecting users to the data that matters to them.

## Entities

TBD

## Security

### HTML Sanitizer
(Dom Purify)[https://www.npmjs.com/package/dompurify] is used in conjunction with ember-auto-import to transform the npm package into an ember consumable format.
DOMPurify is a DOM-only XSS sanitizer for HTML, MathML and SVG.You can feed DOMPurify with string full of dirty HTML and it will return a string (unless configured otherwise) with clean HTML.

## Creating a new package

`ember g addon <package-name>`

## Package Organization

The packages, in order of highest on the dependency tree to lowest:

`data-portal`:
- Contains the host application. This package aggregates the contents of all other packages into the complete
  web client, though it is not intended to have much individual functionality of its own

`@datahub/entities`:
- Contains the entity specific logic and components

`@datahub/shared`:
- Contains features and functionality that applies to the application as a whole, or are shared between more
  than one entity

`@datahub/data-models`:
- Contains the entity definitions and core data management functions that help us define the base properties
  of an entity

`@datahub/metadata-types`:
- Contains logic to translate the backend models to types that we can consume on the client

`@datahub/utils`:
- Core utility functions. This package should not depend on any other DataHub package and instead provides
  base level functions for all other packages.
