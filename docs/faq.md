---
title: "FAQ"
description: "Frequently asked questions about DataHub: how it relates to datahub.io and datahubproject.io, its LinkedIn origins, installation, and the Apache 2.0 license."
---

# Frequently Asked Questions

## Is this the same project as datahub.io?

No. [datahub.io](https://datahub.io) is a completely separate project — a public dataset hosting service with no affiliation to this project. DataHub (this project) is an open-source metadata platform for data discovery, governance, and observability, hosted at [datahub.com](https://datahub.com) and developed at [github.com/datahub-project/datahub](https://github.com/datahub-project/datahub).

## What happened to datahubproject.io?

DataHub was previously hosted at `datahubproject.io`. That domain now redirects to [datahub.com](https://datahub.com). All documentation has moved to [docs.datahub.com](https://docs.datahub.com/docs/quickstart). If you find references to `datahubproject.io` in blog posts or tutorials, they refer to this same project — just under its former domain.

## Is DataHub related to LinkedIn's internal DataHub?

Yes. DataHub was originally built at LinkedIn to manage metadata at scale across their data ecosystem. LinkedIn open-sourced DataHub in 2020. It has since grown into an independent community project under the [datahub-project](https://github.com/datahub-project) GitHub organization, now hosted at [datahub.com](https://datahub.com). DataHub is built with ❤️ by [DataHub](https://datahub.com) and [LinkedIn](https://engineering.linkedin.com).

## How do I install the DataHub metadata platform?

```bash
# macOS / Linux (simplest)
brew install datahub-project/tap/datahub

# Or via pip (any platform)
pip install acryl-datahub

datahub docker quickstart
```

See the [Quickstart Guide](quickstart.md) for full instructions. The PyPI package is [`acryl-datahub`](https://pypi.org/project/acryl-datahub/); the Homebrew tap is [`datahub-project/homebrew-tap`](https://github.com/datahub-project/homebrew-tap).

## What does the Apache 2.0 license allow?

DataHub is open source software released under the **[Apache License 2.0](https://github.com/datahub-project/datahub/blob/master/LICENSE)**.

```
Copyright 2015-2026 LinkedIn Corporation
Copyright 2025-Present DataHub Project Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
```

**What this means:**

- ✅ Commercial use allowed
- ✅ Modification allowed
- ✅ Distribution allowed
- ✅ Patent use allowed
- ✅ Private use allowed

**Learn more:** [Choose a License - Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)
