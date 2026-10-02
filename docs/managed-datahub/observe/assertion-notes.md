

# Assertion Notes

> **Availability:** DataHub Cloud only

> The **Assertion Notes** feature is available as part of the **DataHub Cloud Observe** module of DataHub Cloud.
> If you are interested in learning more about **DataHub Cloud Observe** or trying it out, please [visit our website](https://datahub.com/products/data-observability/).

## Introduction

The Assertion notes feature aims to solve two key use cases:

1. Surfacing useful tips for engineers to troubleshoot and resolve data quality failures
2. Documenting the purpose of a given check, and implications of its failure; for instance, some checks may circuit-break pipelines.

### For Troubleshooting

As you scale your data quality coverage across a large data landscape, you will often find that the engineers who are troubleshooting and resolving an assertion failure are not the same people who created the check.
Oftentimes, it's useful to provide troubleshooting instructions or notes with context about how to resolve the problem when a check fails.

- If the check was manually set up, it may be worthwhile for the creator to add notes for future on-call engineers
- If it was an AI check, whoever is first to investigate the failure may want to document what they did to fix it.

### For Documenting

Adding notes to Assertions is useful for documenting your Assertions. This is particularly relevant for Custom SQL checks, where understanding the logic from the query statements can be difficult. By adding documentation in the notes tab, others can understand exactly what is being monitored and how to resolve issues in event of failure.

[▶ Watch video](https://www.loom.com/share/a6cb07d33e8440acafacea381912f904)
