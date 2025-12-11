# Changelog 

**Repository:** `ndt-data-catalogue`  
**Description:** `Tracks all notable changes, version history, and roadmap toward 1.0.0 following Semantic Versioning.`  
**SPDX-License-Identifier:** `OGL-UK-3.0`  

All notable changes to this repository will be documented in this file.

This project follows **Semantic Versioning (SemVer)** ([semver.org](https://semver.org/)), using the format:


`[MAJOR].[MINOR].[PATCH]` 
- **MAJOR** (`X.0.0`) – Incompatible API/feature changes that break backward compatibility. 
- **MINOR** (`0.X.0`) – Backward-compatible new features, enhancements, or functionality changes. 
- **PATCH** (`0.0.X`) – Backward-compatible bug fixes, security updates, or minor corrections. 
- **Pre-release versions** – Use suffixes such as `-alpha`, `-beta`, `-rc.1` (e.g., `2.1.0-beta.1`). 
- **Build metadata** – If needed, use `+build` (e.g., `2.1.0+20250314`). 

---

## [0.90.0] 

### Initial Public Release

### Added 
- Added new documentation to bring the repo in line with OSPO standards. Files added include ACKNOWLEDGEMENTS.md, CHANGELOG.md, CODE_OF_CONDUCT.md, CONTRIBUTING.md, LICENSE.MD, MAINTAINERS.md, NOTICE.md and OGL_LICENSE.md.
- Added GitHub workflows from the archetypes template repo.
- Added configuration files for skywalking-eyes

### Fixed 
- 

### Changed 
- Modified existing markdown files such as README and SECURITY to bring them in line with OSPO standards and remove links to, and reference to anything specific to the DataHub project.
- Modified CODEOWNERS to reflect new ownership.
- Modified the pull request template to reflect archetype version.
- Modified existing GitHub workflows to reflect the change of branch name from `master` to `main`
- Disabled some of the GitHub workflows that require further investigation and may cause issue for us initially
- Added copyright headers to all required files

---

## Future Roadmap to `1.0.0` 

The `0.90.x` series is part of NDTP’s **pre-stable development cycle**, meaning: 
- **Minor versions (`0.91.0`, `0.92.0`...) introduce features and improvements** leading to a stable `1.0.0`. 
- **Patch versions (`0.90.1`, `0.90.2`...) contain only bug fixes and security updates**. 
- **Backward compatibility is NOT guaranteed until `1.0.0`**, though NDTP aims to minimise breaking changes. 

Once `1.0.0` is reached, future versions will follow **strict SemVer rules**. 

---

## Versioning Policy 

1. **MAJOR updates (`X.0.0`)** – Typically introduce breaking changes that require users to modify their code or configurations. 
- **Breaking changes (default rule)**: Any backward-incompatible modifications require a major version bump. 
- **Non-breaking major updates (exceptional cases)**: A major version may also be incremented if the update represents a significant milestone, such as a shift in governance, a long-term stability commitment, or substantial new functionality that redefines the project’s scope. 
2. **MINOR updates (`0.X.0`)** – New functionality that is backward-compatible. 
3. **PATCH updates (`0.0.X`)** – Bug fixes, performance improvements, or security patches. 
4. **Dependency updates** – A **major dependency upgrade** that introduces breaking changes should trigger a **MAJOR** version bump (once at `1.0.0`). 

---

## How to Update This Changelog 

1. When making changes, update this file under the **Unreleased** section. 
2. Before a new release, move changes from **Unreleased** to a new dated section with a version number. 
3. Follow **Semantic Versioning** rules to categorise changes correctly. 
4. If pre-release versions are used, clearly mark them as `-alpha`, `-beta`, or `-rc.X`. 

---

**Maintained by the National Digital Twin Programme (NDTP).** 

© Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.

Licensed under the Open Government Licence v3.0.

For full licensing terms, see [OGL_LICENCE.md](OGL_LICENCE.md).

