# Overrides to the react code base

Documented for future support in debugging or restoring changes:

## ./public/index.html

- Added new kicon.ico to public/ folder, and replaced favicon.ico with kicon.ico for Klarna favicon in browser.
- Added stylesheet for Klarna fonts.
- Changed text within title-tags to "Lakehouse Data Catalog"

## ./src/conf/theme/global-overrides.less

- Added import of new file klarna-overrides.less

## ./src/conf/theme/global-variables.less

- Added the Klarna font string to the @font-family variable

```
@font-family: 'Klarna Text', 'Helvetica Neue', Arial, Helvetica, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol', 'Noto Color Emoji';
```

## ./src/conf/theme/klarna-overrides.less

- Added button overrides to the following button classes:

```
ant-btn:not(.ant-btn-text, .ant-btn-link)
.ant-btn[disabled]
.ant-btn-default
```