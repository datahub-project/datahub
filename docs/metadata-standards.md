# Metadata Standards

The data and AI tooling and infrastructure stack is constantly evolving and adding new concepts (from datasets to dashboards, to models and training runs). DataHub’s goal is to harmonize this complexity and make it understandable for humans and machines, while not sacrificing fidelity. As a result, [over 10 years of iteration](https://www.linkedin.com/blog/engineering/data-management/datahub-popular-metadata-architectures-explained), the DataHub project has evolved into a comprehensive living metadata model that serves as a de-facto standard for metadata in the data and AI stack.

While other standards exist - like the Iceberg Catalog API and OpenLineage - DataHub implements their interfaces so you can continue using familiar tools while maintaining a single source of truth.

<p align="center">
<a href="https://xkcd.com/927/"><img max-width="80%" src="https://imgs.xkcd.com/comics/standards.png" alt="xkcd comic on standards" /></a>
</p>

So instead of “yet another standard to understand and maintain” like the all too familiar example above - we reduce complexity in the stack.

## The DataHub Metadata Standard

Modern data and AI teams need metadata that's comprehensive, flexible, and accessible. DataHub's [open metadata standard](./modeling/metadata-model.md) delivers this through:

- A complete model that encompasses business and technical metadata
- A schema-first approach to modeling that enables easy extensibility
- Open-source standards and APIs that prevent vendor lock-in

Metadata is the shared backbone for data discovery, governance, and observability - so it’s important to get these interfaces right. DataHub draws on over 10 years of experience building metadata-driven systems at LinkedIn, and so our [third-generation platform](https://www.linkedin.com/blog/engineering/data-management/datahub-popular-metadata-architectures-explained) has been able to learn from previous modeling mistakes.

### Aside: Business vs Technical Catalogs

Historically, the data catalog world has been split in two: business catalogs for human-facing data discovery and documentation, and technical catalogs / meta-stores meant to be machine-facing [operational catalogs for query / compute engines](https://www.youtube.com/watch?v=yPqSR18BzO4).

DataHub aims to bring these two worlds a bit closer together. Often companies end up operating both business and technical catalogs side by side - and the overlap between them causes messy bidirectional synchronization problems and unnecessary repetition in authentication and authorization. In many of these cases, there should just be a unified tool that does both.

Being a unified catalog requires a unified metadata standard and a unified product experience. It needs an understandable user interface that uses progressive disclosure to enable power users without overwhelming casual users, and powerful APIs and SDKs to support tools and automations. To our knowledge, DataHub is the only fully open source platform that does this.

## Interop with other standards

Given that DataHub is the only open source unified metadata platform, our metadata standard is naturally a superset of most other metadata standards.

As such, our general approach is to embrace and implement their interfaces, but automatically translate them to the more comprehensive DataHub metadata model for storage.

We currently implement the Iceberg REST Catalog API and the OpenLineage API.

### Apache Iceberg REST Catalog

The Iceberg REST Catalog API is the standard interface for managing table metadata in Apache Iceberg, a popular table format for data lakes. It provides a REST interface for creating and managing Iceberg tables, including their schema, partitioning, and other metadata.

**How DataHub implements it:**

- DataHub serves as a drop-in Iceberg REST Catalog implementation.
- Once configured, you can use Iceberg exactly as you normally would.
- Tables created via Iceberg automatically appear in DataHub with full metadata.
- All table operations are available through both Iceberg and DataHub's interfaces.

Learn more about the integration in the [Iceberg REST catalog docs](./iceberg-catalog.md).

### OpenLineage

OpenLineage is an evolving standard for collecting lineage metadata from data pipelines. It defines a common specification for capturing run-time information about data processing jobs, including inputs, outputs, and job context.

**How DataHub implements it:**

- DataHub natively accepts OpenLineage events through its API.
- Events are automatically translated into DataHub's richer metadata model.
- All lineage information is immediately available in DataHub's UI and APIs.
- While we support OpenLineage, our native integrations offer enhanced capabilities. For instance, the DataHub Airflow plugin uses schema-aware SQL parsing to generate more accurate column-level lineage.

Learn more about the integration in the [OpenLineage integration docs](./lineage/openlineage.md).
