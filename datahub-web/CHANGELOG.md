# Changelog

All changes to the this project will be documented here.

### Types of Changes
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project *does not adhere* to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2018-11-05]
### Added
- Created new addon in nacho/ for user avatars
- Created new addon in nacho/ for dropdowns
- Created new addon in nacho/ for power select + lookup search

### Changed
- Reorganized dependent packages and removed yarn.lock files consistent with previous changes for Yarn Workspaces

## [2018-11-05]
### Added
- Hoisted package dependencies to monorepo by letting lerna defer to Yarn Workspaces. However, some package bundlers are currently incompatible with dependencies being hoisted to the root of the monorepo, they assume that dependent modules reside under the application's node_modules folder during installation and runtime, which conflicts with Yarn Workspaces scheme. The nohoist feature helps to resolve this issue.
- Added a CHANGELOG.md file to packages and the monorepo root.
