# DataHub Docs Overview

DataHub's project documentation is hosted at [datahubproject.io](https://datahubproject.io/docs)

## Types of Documentation

### Feature Guide

A Feature Guide should follow the [Feature Guide Template](/_feature-guide-template.md), and should provide the following value:

* At a high level, what is the concept/feature within DataHub?
* Why is the feature useful?
* What are the common use cases of the feature?
* What are the simple steps one needs to take to use the feature?

When creating a Feature Guide, please remember to:

* Provide plain-language descriptions for both technical and non-technical readers
* Avoid using industry jargon, abbreviations, or acryonyms
* Provide descriptive screenshots, links out to relevant YouTube videos, and any other relevant resources
* Provide links out to Tutorials for advanced use cases

*Not all Feature Guides will require a Tutorial.*

### Tutorial

A Tutorial is meant to provide very specific steps to accomplish complex workflows and advanced use cases that are out of scope of a Feature Guide.

Tutorials should be written to accomodate the targeted persona, i.e. Developer, Admin, End-User, etc.

*Not all Tutorials require an associated Feature Guide.*

## Docs Best Practices

### Embedding GIFs and or Screenshots

* Store GIFs and screenshots in [datahub-project/static-assets](https://github.com/datahub-project/static-assets); this minimizes unnecessarily large image/file sizes in the main repo
* Center-align screenshots and size down to 70% - this improves readability/skimability within the site

Example snippet:

```
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/impact-analysis-export-full-list.png"/>
</p>
```

* Use the "raw" GitHub image link (right click image from GitHub > Open in New Tab > copy URL):

  * Good: https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-test-logic-view.png
  * Bad: https://github.com/datahub-project/static-assets/blob/main/imgs/dbt-test-logic-view.png