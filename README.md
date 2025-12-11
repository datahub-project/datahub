# README  

**Repository:** `ndt-data-catalogue`  
**Description:** `This repository contains the code required to build and run the NDT data catalogue`  
**SPDX-License-Identifier:** `Apache-2.0 AND OGL-UK-3.0 ` 

# National Digital Twins (NDT) Data Catalogue

## Overview  

This repository has been put together to house the open sourced code required to build and run the NDT Data Catalogue.

The NDT Data Catalogue is used to view data products held within a node net, which is made up of all products produced by organisations within the node net. Users will be able to look at a products metadata, lineage and more.

This repository is a fork of [datahub-project/datahub](https://github.com/datahub-project/datahub) that will be reskinned and developed for use within the NDT program and infrastructure.

## Prerequisites  

The prerequisites can be found on the [DataHub Quickstart Guide](./docs/quickstart.md).

## Quickstart

Please follow the [DataHub Quickstart Guide](./docs/quickstart.md) to run DataHub locally using [Docker](https://docker.com).

## Pull Requests

Included in this repository is an example [PULL_REQUEST_TEMPLATE.md](./.github/PULL_REQUEST_TEMPLATE.md), which can be used to help prompt specific content to include in pull requests by contributors. 

## Development

As we are going to modify datahub to build the NDT Data Catalogue, please take a look at the [Development Guide](./docs/quickstart.md). There are various other documents within this repo in the `docs` folder that may also be of interest.

## Other Source Code and Repositories from DataHub

- [datahub-project/datahub](https://github.com/datahub-project/datahub): This repository contains the complete source code for DataHub's metadata model, metadata services, integration connectors and the web application. (the repo this repo was forked from)
- [acryldata/datahub-actions](https://github.com/acryldata/datahub-actions): DataHub Actions is a framework for responding to changes to your DataHub Metadata Graph in real time.
- [acryldata/datahub-helm](https://github.com/acryldata/datahub-helm): Helm charts for deploying DataHub on a Kubernetes cluster
- [acryldata/meta-world](https://github.com/acryldata/meta-world): A repository to store recipes, custom sources, transformations and other things to make your DataHub experience magical.
- [dbt-impact-action](https://github.com/acryldata/dbt-impact-action): A github action for commenting on your PRs with a summary of the impact of changes within a dbt project.
- [datahub-tools](https://github.com/makenotion/datahub-tools): Additional python tools to interact with the DataHub GraphQL endpoints, built by Notion.
- [business-glossary-sync-action](https://github.com/acryldata/business-glossary-sync-action): A github action that opens PRs to update your business glossary yaml file.
- [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub): A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for DataHub.

## Public Funding Acknowledgment  
This repository has been developed with public funding as part of the National Digital Twin Programme (NDTP), a UK Government initiative. NDTP, alongside its partners, has invested in this work to advance open, secure, and reusable digital twin technologies for any organisation, whether from the public or private sector, irrespective of size.  

## License  
This repository contains both source code and documentation, which are covered by different licenses:  
- **Code:** Originally developed by DataHub and LinkedIn, now maintained by National Digital Twin Programme. Licensed under the [Apache License 2.0](./LICENSE.md).
- **Documentation:** Licensed under the [Open Government Licence v3.0](./OGL_LICENCE.md).

See [`LICENSE.md`](LICENSE.md), [`OGL_LICENCE.md`](OGL_LICENCE.md) and [`NOTICE.md`](NOTICE.md) for details.

## Security and Responsible Disclosure
We take security seriously. If you believe you have found a security vulnerability in this repository, please follow our responsible disclosure process outlined in [`SECURITY.md`](./SECURITY.md).  

## Software Bill of Materials (SBOM)
This project provides a Software Bill of Materials (SBOM) to help users and integrators understand its dependencies.

### Current SBOM
Download the [latest SBOM for this codebase](../../dependency-graph/sbom) to view the current list of components used in this repository.

## Contributing  
We welcome contributions that align with the Programme’s objectives. Please read our [`CONTRIBUTING.md`](./CONTRIBUTING.md) guidelines before submitting pull requests.  

## Acknowledgements  
This repository has benefited from collaboration with various organisations. For a list of acknowledgments, see [`ACKNOWLEDGEMENTS.md`](./ACKNOWLEDGEMENTS.md).  

## Support and Contact  
For questions or support, check our Issues or contact the NDTP team on ndtp@businessandtrade.gov.uk.

For any questions or support regarding DataHub and its usage/development, please contact the DataHub team using the DataHub [Development Guide](./docs/quickstart.md)

**Maintained by the National Digital Twin Programme (NDTP).**  

© Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
Licensed under the Open Government Licence v3.0.