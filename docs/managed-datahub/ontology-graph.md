---
title: Ontology Graph
description: "Explore how glossary terms, domains, data products, and tagged assets relate to each other in DataHub Cloud."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Ontology Graph

<FeatureAvailability saasOnly />

The Ontology Graph is a DataHub Cloud view of how business meaning connects to the catalog. It is not lineage: edges are glossary relationships, domain membership, data product membership, containment, and glossary tags — not `DownstreamOf`.

You can open it in two places:

1. **Govern → Ontology** in the left nav (`/ontology`). This lands on your glossary roots — term groups and terms with no parent — the same starting set as Business Glossary home.
2. The **Relationships** tab on a glossary term, glossary term group, domain, or data product. That seeds the graph on the entity you are looking at and expands one hop.

## What appears on the graph

Nodes are glossary term groups, glossary terms, domains, data products, containers, and datasets. Columns are not separate nodes. When a glossary term is applied to a column, that column is shown on the parent dataset (or container) and the edge attaches to the column.

The layout is top-down: term groups, then terms, then domains, data products, containers, and datasets.

Use the type filter to hide kinds of assets you do not want in the current view.

## Expanding neighbors

Each node can expand up (more conceptual / parent) and down (more physical / child) one hop at a time. Carets appear after DataHub has probed that side, and only if there are neighbors to show.

Paging matches lineage: the first page is 4 neighbors, then 10 at a time. Hover the caret for **Show All** (when the rest fits in 100) or **Show +100**.

Double-click a node to open its entity profile.

## Related features

- [Business Glossary](../glossary/business-glossary.md)
- [Domains](../domains.md)
- [Data Products](../dataproducts.md)
