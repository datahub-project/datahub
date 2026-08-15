---
description: "Roll tags, glossary terms, owners, domains, and structured properties up an asset's hierarchy — and optionally back down onto datasets in a container — using DataHub Cloud's Hierarchy Propagation automations."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Hierarchy Propagation Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in Public Beta in DataHub Cloud. Reach out to your DataHub Cloud representative to get access.

:::

## Introduction

Hierarchy Propagation is an automation that rolls an asset's metadata **up** its physical and logical hierarchy, and optionally **back down** onto the datasets a container holds. It keeps a warehouse subtree — or a data product or domain — consistently classified without tagging every table by hand.

This is different from [Glossary Term Propagation](./glossary-term-propagation.md), which flows labels **downstream along lineage**. Hierarchy Propagation follows containers, data products, domains, and applications.

There is one automation per payload: **Glossary Terms**, **Tags**, **Owners**, **Domain**, and **Structured Properties**. They share the same targets and behaviour. This automation is available in DataHub Cloud only.

## Capabilities

- **Roll up from a column, dataset, or container.** A column rolls onto its parent dataset and that dataset's ancestors. A dataset rolls onto its containers, data products, domain, and applications. A container rolls onto its parent container(s) and its own logical groupings.
- **Roll back down onto data sources.** When contained datasets are enabled, a value on a container is pushed onto every dataset it contains. Combined with upward roll-up, a value applied to any table is aggregated to the shared container and shared with sibling tables.
- **Physical and logical targets.** Choose containers, data products, domains, applications, or any combination.
- **Attributed writes.** Propagated values show the thunderbolt marker. Removing them does not touch values a person applied directly.
- **Optional scoping.** Restrict glossary-term, tag, or structured-property roll-up to specific values. Owner and domain automations propagate every change of that type — they have no scoping filter.

## Enabling Hierarchy Propagation

1. **Navigate to Automations**: Go to **Govern > Automations** in the navigation bar.

<p align="center">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
</p>

2. **Create an Automation**: Select the Hierarchy Propagation type for the metadata you want — Glossary Terms, Tags, Owners, Domain, or Structured Properties.

3. **Configure the automation**: Give it a name, choose where values should roll (parent containers, contained datasets, data products, domain, applications), and — for terms, tags, or structured properties — optionally scope which values propagate. Select **Save and Run** to activate it.

## Propagating for Existing Assets

In DataHub Cloud, you can back-fill historical data so existing values are rolled up (and down) across the hierarchy. Open the automation created above, click the 3-dot **more** menu, and choose **Initialize**.

<p align="left">
  <img width="15%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

then click **Initialize**.

<p align="left">
  <img width="15%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

Skip this step if you only want to propagate values going forward.

## Viewing Propagated Metadata

Propagated tags, terms, owners, domains, and structured properties display a thunderbolt icon. The tooltip shows where the value originated.

<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/glossary-term-propagation/view-propagated-terms.png"/>
</p>

## How It Works

- **Attribution.** Every propagated value records this automation as the source, the origin asset that changed, and the direction (up or down). Values a user applied directly are never overwritten.
- **Up, then down.** A steward adds `Confidential` to `db.schema.events`. The term rolls **up** onto the schema and database containers, then — if contained datasets are enabled — **down** onto the other tables in that schema.
- **Safe removal.** When a value is removed from a source, the propagated copy is removed from an upward target only once no other member of that target still carries it. Removing a value from a **container** strips the rolled-down copy from every dataset below it. Hand-applied values are left untouched.
- **Keep it bounded.** Combined up-and-down on containers spreads a value across the whole physical subtree. Scope term, tag, or structured-property automations to the classifications you actually want to share, or enable only one direction.

## Troubleshooting

- **A value did not roll down.** Contained-dataset roll-down only runs when the source of the event is a container. A value applied to a dataset first rolls up to its containers; the downward pass then runs off that container change.
- **Structured-property conflicts.** A structured property holds one assignment per property, so a downward target reflects the most recent origin rather than a union.
