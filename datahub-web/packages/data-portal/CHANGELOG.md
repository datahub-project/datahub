# Changelog

All changes to the this project will be documented here.

### Types of Changes

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project _does not adhere_ to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2018-11-14]

### Added

- UMP Metrics Profile Page Routing and Entity Header.
  Current pending features include Metric ownership, Overview and Configuration property tables

### Fixed

- Import of styles from @datahub/entity-page component

## [2018-11-05]

### Removed

- A `yarn.lock` file is no longer required per monorepo package since Yarn Workspaces hoists dependencies to the root when possible and can resolve references using the lock file there.
- Unused dependencies: CsvToMarkdown.js, scrollmonitor, json.human
- Removed manual import for font-awesome in ember-cli-build script

### Changed

- Upgrades typescript to latest @3.1.6
