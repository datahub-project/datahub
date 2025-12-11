# Contribution Guidelines  

**Repository:** `ndt-data-catalogue`   
**Description:** `Guidelines for issue reporting, documentation suggestions, and NDTP’s controlled contribution model.`  
**SPDX-License-Identifier:** `OGL-UK-3.0`  

Thank you for your interest in this repository.  

The National Digital Twin Programme (NDTP) develops and maintains this repository in collaboration with suppliers and partner organisations, including other parts of government and their suppliers.  

NDTP follows an **open-source governance model** where all code is **publicly available** under open-source licences, and collaboration is invited from **approved partners**. Contributions from the general public are not currently accepted, but **feedback, issue reporting, and documentation suggestions are encouraged**.  

If you want to see which suppliers and organisations have contributed to this repository in the past, refer to [ACKNOWLEDGEMENTS.md](./ACKNOWLEDGEMENTS.md) and the GitHub contributor insights page at [Contributors](../../graphs/contributors).

---

## How You Can Contribute  

Public users and NDTP partners are encouraged to engage in the following ways:  

- **Reporting bugs and issues** – If you find a problem, please open a GitHub issue.  
- **Suggesting documentation improvements** – Propose clarifications or additions to the existing documentation.  
- **Providing structured feedback** – If you have suggestions for improvements, let us know via GitHub Issues.  

While we review all input, NDTP prioritises development based on programme goals, supplier development cycles, and strategic objectives.  

NDTP does not currently accept **public pull requests (PRs) or direct code contributions** to this repository. Contributions are limited to **approved suppliers and partner organisations** under formal agreements.  

For details on repository maintainers and how to contact them, refer to [MAINTAINERS.md](./MAINTAINERS.md).  

---

## Commit Requirements

For commits on this repository we are using `commitlint` to enforce the [Conventional Commits](https://www.conventionalcommits.org/) specification. This keeps the git history clean and readable, and also allows for automated tools to assist in generating changelogs in the future. 

The commit message should be structured as follows:

```text
Subject:
<type>: <short summary>
  │            │
  │            └─⫸ Summary in present tense. Not capitalized. No period at the end.
  │       
  │       
  │
  └─⫸ Commit Type: build|docs|feat|fix|perf|refactor|revert|test

Body:
<detailed description of changes made in the commit> (wrap at 72 characters)

Footer:
<any additional information, such as references or issue numbers>
```

| Type | Description | SemVer Impact |
| - | - | - |
| `build` | A change to CI configuration files and scripts, or that affect the build system or external dependencies | None (*unless functionality is affected*) |
| `chore` | Changes to the build process or auxiliary tools and libraries such as documentation generation |
| `docs` | Documentation only changes | None |
| `feat` | A new feature | MINOR (`x.Y.z`) |
| `fix` | A bug fix | PATCH (`x.y.Z`) |
| `perf` | A code change that improves performance | PATCH (`x.y.Z`) |
| `refactor` | A code change that improve code quality but have no functional effect | None (*unless functionality is affected*) |
| `revert` | Reverts a previous commit | Depends on the reverted change |
| `style` | Changes that do not affect the meaning of the code (white-space, formatting, etc) |
| `test` | Adding or correcting tests | None |

## Reporting Issues  

If you encounter a bug, error, or inconsistency, please follow these steps:  

1. Check for an existing issue under [Issues](../../issues).  
2. Open a new issue if no one has reported it yet. Use one of the provided issue templates.  
3. Provide a clear, detailed description of the issue, including steps to reproduce it if applicable.  
4. Label the issue appropriately (bug, documentation, enhancement, etc.).  

For security-related issues, do not submit a public issue. Instead, follow our [Responsible Disclosure process](./SECURITY.md).  

---

## Documentation Feedback  

If you find an error in the documentation, need more clarity, or have suggestions for additional documentation, you can:  

1. Open a GitHub issue under the `documentation` label.  
2. Describe the improvement you are suggesting, including references to existing documentation where applicable.  
3. Submit structured feedback – specific examples help us make updates faster.  

We prioritise documentation updates based on user impact and alignment with programme goals.  

---

## NDTP's Approach to Open-Source Development  

- **All NDTP code is publicly available under open-source licences.**  
- **Development is led by approved suppliers and partners** who have been engaged through a formal process.  
- **We welcome feedback and ideas**, but implementation is subject to programme priorities.  

To see what we’re working on, check out our [Project Roadmap](../../projects). If no roadmap is currently available, please note that it is being actively developed and will be published in due course.  

---

## Branching Strategy  

This repository follows a **GitFlow-based branching model** to manage development efficiently. Key conventions include:  

- **Main Branch (`main`)**: The stable, production-ready branch. Only tested and approved changes are merged here.  
- **Develop Branch (`develop`)**: The integration branch where features and fixes are merged before reaching `main`.  
- **Feature Branches (`feature/*`)**: Used for new developments. Named based on functionality, e.g., `feature/new-auth-method`.  
- **Bugfix Branches (`bugfix/*`)**: Address minor issues in `develop` before release.  
- **Release Branches (`release/*`)**: Used to prepare a new stable release, ensuring final testing and versioning updates.  
- **Hotfix Branches (`hotfix/*`)**: Critical fixes applied directly to `main` and merged back into `develop`.  

For more details, refer to [GitFlow Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow).  

---

## Pull Request Policy  

To maintain high-quality contributions, NDTP enforces the following **minimum pull request (PR) requirements** for approved contributors:  

- **All PRs must be reviewed by at least one maintainer** before merging.
- **PRs should reference a corresponding issue** where applicable.
- **Code changes must include relevant tests** to ensure stability.
- **Commit messages should follow best practices**, including referencing issue numbers when relevant.
- **Documentation updates should accompany PRs that impact functionality.**
- **PRs should use "squash and merge" as the preferred merge strategy**, ensuring a clean history.
- **Feature and bugfix branches should be deleted after merge** to keep the repository tidy.
- **Force pushing to the `main` branch is strictly prohibited** to protect repository integrity.
- **CI builds must pass before merging** to enforce basic validation checks.

---

## Contribution Licensing  

By submitting feedback, documentation suggestions, or issue reports, you acknowledge that any resulting changes will be licensed under the same open-source terms as this repository:  

- Code contributions (if ever accepted) will be licensed under Apache 2.0.  
- Documentation updates will be licensed under OGL v3.0.  

For supplier-contracted development, NDTP ensures that all contributions align with Crown Copyright and public sector open-source standards.  

---

## Repository Maintainers  

For details on who maintains this repository and how to contact them, refer to [MAINTAINERS.md](./MAINTAINERS.md).  

NDTP repository maintainers review reported issues, evaluate documentation suggestions, and oversee ongoing development.  

---

**Maintained by the National Digital Twin Programme (NDTP).**

© Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.  

Licensed under the Open Government Licence v3.0.  

For full licensing terms, see [OGL_LICENCE.md](./OGL_LICENCE.md)
