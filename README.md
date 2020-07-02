# DataHub: A Generalized Metadata Search & Discovery Tool
[![Version](https://img.shields.io/github/v/release/linkedin/datahub?include_prereleases)](https://github.com/linkedin/datahub/releases)
[![Build Status](https://travis-ci.org/linkedin/datahub.svg)](https://travis-ci.org/linkedin/datahub)
[![Get on Slack](https://img.shields.io/badge/slack-join-orange.svg)](https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/linkedin/datahub/blob/master/CONTRIBUTING.md)
[![License](https://img.shields.io/github/license/linkedin/datahub)](LICENSE)

---

[Quickstart](#quickstart) |
[Documentation](#documentation) |
[Features](https://github.com/linkedin/datahub/blob/master/docs/features.md) |
[Roadmap](https://github.com/linkedin/datahub/blob/master/docs/roadmap.md) |
[FAQ](https://github.com/linkedin/datahub/blob/master/docs/faq.md) |
[Town Hall](https://github.com/linkedin/datahub/blob/master/docs/townhalls.md)

---

![DataHub](docs/imgs/datahub-logo.png)

> :mega: Next DataHub town hall meeting on July 31st, 9am-10am PDT: 
> - [Signup sheet & questions](https://docs.google.com/spreadsheets/d/1hCTFQZnhYHAPa-DeIfyye4MlwmrY7GF4hBds5pTZJYM)
> - Details and recordings of past meetings can be found [here](docs/townhalls.md)

> :sparkles:Latest Update: 
> - We released v0.4.1, you can find release notes [here](https://github.com/linkedin/datahub/releases/tag/v0.4.1)
> - We're on Slack now! [Join](https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA) or [log in with an existing account](https://datahubspace.slack.com). Ask questions and keep up with the latest announcements.

## Introduction
DataHub is LinkedIn's generalized metadata search & discovery tool. To learn more about DataHub, check out our 
[LinkedIn blog post](https://engineering.linkedin.com/blog/2019/data-hub) and [Strata presentation](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019). 
You should also visit [DataHub Architecture](docs/architecture/architecture.md) to get a better understanding of how DataHub is implemented and [DataHub Onboarding Guide](docs/how/entity-onboarding.md) to understand how to extend DataHub for your own use case.

This repository contains the complete source code for both DataHub's frontend & backend. You can also read about [how we sync the changes](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) between our the internal fork and GitHub. 

## Quickstart
1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/) (if using Linux). Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs, 8GB RAM, 2GB Swap area.
2. Open Docker either from the command line or the desktop app and ensure it is up and running.
3. Clone this repo and `cd` into the root directory of the cloned repository.
4. Run the following command to download and run all Docker containers locally:
    ```
    ./docker/quickstart/quickstart.sh
    ```
    This step takes a while to run the first time, and it may be difficult to tell if DataHub is fully up and running from the combined log. Please use [this guide](https://github.com/linkedin/datahub/blob/master/docs/debugging.md#how-can-i-confirm-if-all-docker-containers-are-running-as-expected-after-a-quickstart) to verify that each container is running correctly.
5. At this point, you should be able to start DataHub by opening [http://localhost:9001](http://localhost:9001) in your browser. You can sign in using `datahub` as both username and password. However, you'll notice that no data has been ingested yet.
6. To ingest provided [sample data](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/mce-cli/bootstrap_mce.dat) to DataHub, switch to a new terminal window, `cd` into the cloned `datahub` repo, and run the following command:
    ```
    ./docker/ingestion/ingestion.sh
    ```
   After running this, you should be able to see and search sample datasets in DataHub.

Please refer to the [debugging guide](docs/debugging.md) if you encounter any issues during the quickstart.

## Documentation
* [DataHub Developer's Guide](docs/developers.md)
* [DataHub Architecture](docs/architecture/architecture.md)
* [DataHub Onboarding Guide](docs/how/entity-onboarding.md)
* [Docker Images](docker)
* [Frontend](datahub-frontend)
* [Web App](datahub-web)
* [Generalized Metadata Service](gms)
* [Metadata Ingestion](metadata-ingestion)
* [Metadata Processing Jobs](metadata-jobs)

## Releases
See [Releases](https://github.com/linkedin/datahub/releases) page for more details. We follow the [SemVer Specification](https://semver.org) when versioning the releases and adopt the [Keep a Changelog convention](https://keepachangelog.com/) for the changelog format.

## FAQs
Frequently Asked Questions about DataHub can be found [here](https://github.com/linkedin/datahub/blob/master/docs/faq.md).

## Features & Roadmap
Check out DataHub's [Features](docs/features.md) & [Roadmap](docs/roadmap.md).

## Contributing
We welcome contributions from the community. Please refer to our [Contributing Guidelines](CONTRIBUTING.md) for more details. We also have a [contrib](contrib) directory for incubating experimental features. 

## Community
Join our [slack workspace](https://app.slack.com/client/TUMKD5EGJ/DV0SB2ZQV/thread/GV2TEEZ5L-1583704023.001100) for important discussions and announcements. You can also find out more about our past and upcoming [town hall meetings](https://github.com/linkedin/datahub/blob/master/docs/townhalls.md).

## Related Articles & Presentations
* [DataHub: A Generalized Metadata Search & Discovery Tool](https://engineering.linkedin.com/blog/2019/data-hub)
* [Open sourcing DataHub: LinkedIn’s metadata search and discovery platform](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p)
* [The evolution of metadata: LinkedIn’s story @ Strata Data Conference 2019](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019)
* [Journey of metadata at LinkedIn @ Crunch Data Conference 2019](https://www.youtube.com/watch?v=OB-O0Y6OYDE)
* [DataHub Journey with Expedia Group by Arun Vasudevan](https://www.youtube.com/watch?v=ajcRdB22s5o)
* [Data Catalogue — Knowing your data](https://medium.com/albert-franzi/data-catalogue-knowing-your-data-15f7d0724900)
* [LinkedIn Datahub Application Architecture Quick Understanding](https://medium.com/@liangjunjiang/linkedin-datahub-application-architecture-quick-understanding-a5b7868ee205)
* [How LinkedIn, Uber, Lyft, Airbnb and Netflix are Solving Data Management and Discovery for Machine Learning Solutions](https://towardsdatascience.com/how-linkedin-uber-lyft-airbnb-and-netflix-are-solving-data-management-and-discovery-for-machine-9b79ee9184bb)
* [Data Discovery in 2020](https://medium.com/@torokyle/data-discovery-in-2020-3c907383caa0)
* [Work-Bench Snapshot: The Evolution of Data Discovery & Catalog](https://medium.com/work-bench/work-bench-snapshot-the-evolution-of-data-discovery-catalog-2f6c0425616b)
* [In-house Data Discovery platforms](https://datastrategy.substack.com/p/in-house-data-discovery-platforms)
* [A Data Engineer’s Perspective On Data Democratization](https://towardsdatascience.com/a-data-engineers-perspective-on-data-democratization-a8aed10f4253)
* [25 Hot New Data Tools and What They DON’T Do](https://blog.amplifypartners.com/25-hot-new-data-tools-and-what-they-dont-do/)
* [4 Data Trends to Watch in 2020](https://medium.com/memory-leak/4-data-trends-to-watch-in-2020-491707902c09)
* [LinkedIn元数据之旅的最新进展—Data Hub](https://zhuanlan.zhihu.com/p/80459081)
* [数据治理篇: 元数据之datahub-概述](https://www.jianshu.com/p/04630b0c63f7)
* [DataHub——实时数据治理平台](https://www.cnblogs.com/tree1123/p/12840871.html)
* [LinkedIn gibt die Datenplattform DataHub als Open Source frei](https://www.heise.de/developer/meldung/LinkedIn-gibt-die-Datenplattform-DataHub-als-Open-Source-frei-4663773.html)
* [Linkedin bringt Open-Source-Datahub](https://www.itmagazine.ch/artikel/71532/Linkedin_bringt_Open-Source-Datahub.html)
