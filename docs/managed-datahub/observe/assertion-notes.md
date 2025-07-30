---
description: This page provides an overview of using Assertion Notes
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Assertion Notes

<FeatureAvailability saasOnly />

> The **Assertion Notes** feature is available as part of the **DataHub Cloud Observe** module of DataHub Cloud.
> If you are interested in learning more about **DataHub Cloud Observe** or trying it out, please [visit our website](https://datahub.com/products/data-observability/).

## Introduction

The Assertion notes feature aims to solve two key use cases:

1. Surfacing useful tips for engineers to troubleshoot and resolve data quality failures
2. Documenting the purpose of a given check, and implications of its failiure; for instance, some checks may circuit-break pipelines.

### For Troubleshooting

As you scale your data quality coverage across a large data lanscape, you will often find that the engineers who are troubleshooting and resolving an assertion failure, are not the people who created the check.
Other times, the Assertion may be an AI anomaly Smart Assertion, so nobody will know exactly how to troubleshoot issues on that table.
In either case, adding a note providing context on how to troubleshoot this check is key.

- If the check was manually set up, it may be worthwhile for the creator to add notes for future on-call engineers
- If it was an AI check, whoever is first to investigate the failure may want to document what they did to fix it.

### For documentation

This is especially useful for things like Custom SQL checks, where reading the query can get tricky. By adding documentation in the notes tab, others can understand exactly what is being monitored, and what the purpose of it is.

<iframe width="516" height="342" src="https://www.loom.com/embed/a6cb07d33e8440acafacea381912f904?sid=32918cd5-9ebf-4aa0-90bc-37fae84d1841" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen></iframe>
