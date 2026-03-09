## Overview

Ldap is an identity and access management platform. Learn more in the [official Ldap documentation](https://ldap.com/).

The DataHub integration for Ldap covers identity entities such as users, groups, and memberships. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                         | DataHub Concept     | Notes                                                            |
| -------------------------------------- | ------------------- | ---------------------------------------------------------------- |
| Ownership and collaboration principals | CorpUser, CorpGroup | Emitted by modules that support ownership and identity metadata. |
