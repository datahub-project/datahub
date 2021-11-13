<!--HOSTED_DOCS_ONLY
import useBaseUrl from '@docusaurus/useBaseUrl';

export const Logo = (props) => {
  return (
    <div style={{ display: "flex", justifyContent: "center", padding: "20px" }}>
      <img
        height="150"
        alt="DataHub Logo"
        src={useBaseUrl("/img/datahub-logo-color-mark.svg")}
        {...props}
      />
    </div>
  );
};

<Logo />

<!--
HOSTED_DOCS_ONLY-->
<p align="center">
<img alt="DataHub" src="docs/imgs/datahub-logo-color-mark.svg" height="150" />
</p>
<!-- -->

# DataHub: The Metadata Platform for the Modern Data Stack
## Built with ❤️ by <img src="https://datahubproject.io/img/acryl-logo-light-mark.png" width="25"/> [Acryl Data](https://acryldata.io) and <img src="https://datahubproject.io/img/LI-In-Bug.png" width="25"/> [LinkedIn](https://engineering.linkedin.com)
[![Version](https://img.shields.io/github/v/release/linkedin/datahub?include_prereleases)](https://github.com/linkedin/datahub/releases/latest)
[![PyPI version](https://badge.fury.io/py/acryl-datahub.svg)](https://badge.fury.io/py/acryl-datahub)
[![build & test](https://github.com/linkedin/datahub/workflows/build%20&%20test/badge.svg?branch=master&event=push)](https://github.com/linkedin/datahub/actions?query=workflow%3A%22build+%26+test%22+branch%3Amaster+event%3Apush)
[![Docker Pulls](https://img.shields.io/docker/pulls/linkedin/datahub-gms.svg)](https://hub.docker.com/r/linkedin/datahub-gms)
[![Slack](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://slack.datahubproject.io)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/linkedin/datahub/blob/master/docs/CONTRIBUTING.md)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/linkedin/datahub)](https://github.com/linkedin/datahub/pulls?q=is%3Apr)
[![License](https://img.shields.io/github/license/linkedin/datahub)](https://github.com/linkedin/datahub/blob/master/LICENSE)
[![YouTube](https://img.shields.io/youtube/channel/subscribers/UC3qFQC5IiwR5fvWEqi_tJ5w?style=social)](https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w)
[![Medium](https://img.shields.io/badge/Medium-12100E?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/datahub-project)
[![Follow](https://img.shields.io/twitter/follow/datahubproject?label=Follow&style=social)](https://twitter.com/datahubproject)
### 🏠 Project Homepage: [datahubproject.io](https://datahubproject.io/)

---

[Quickstart](https://datahubproject.io/docs/quickstart) |
[Documentation](https://datahubproject.io/docs/) |
[Features](https://datahubproject.io/docs/features) |
[Roadmap](https://datahubproject.io/docs/roadmap) |
[Adoption](#adoption) |
[Demo](https://datahubproject.io/docs/demo) |
[Town Hall](https://datahubproject.io/docs/townhalls)

---
> 📣 Next DataHub town hall meeting on Nov 19th, 9am-10am PDT ([convert to your local time](https://greenwichmeantime.com/time/to/pacific-local/))
>
> - Topic Proposals: [submit here](https://docs.google.com/forms/d/1v2ynbAXjJlqY97xE_X1DAntNrXDznOFiNfryUkMPtkI/)
> - Signup to get a calendar invite: [here](https://docs.google.com/forms/d/1r9bObXKS3tgKpISqqO3rw4yQog5zwuaFxg8IrJGUbvQ/)
> - Town-hall Zoom link: [zoom.datahubproject.io](https://zoom.datahubproject.io)
> - [Meeting details](docs/townhalls.md) & [past recordings](docs/townhall-history.md)

> ✨ Latest Update:
>
> - Monthly project update: [August 2021 Edition](https://medium.com/datahub-project/datahub-project-updates-7a0b75cae2b7?source=friends_link&sk=307c9c9983a2d0c778d0455fef12e1e9).
> - Bringing The Power Of The DataHub Real-Time Metadata Graph To Everyone At Acryl Data: [Data Engineering Podcast](https://www.dataengineeringpodcast.com/acryl-data-datahub-metadata-graph-episode-230/)
> - Unleashing Excellent DataOps with LinkedIn DataHub: [DataOps Unleashed Talk](https://www.youtube.com/watch?v=ccsIKK9nVxk).
> - Latest blog post [DataHub: Popular Metadata Architectures Explained](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained) @ LinkedIn Engineering Blog.
> - We're on [Slack](docs/slack.md) now! Ask questions and keep up with the latest announcements.

## Introduction

DataHub is an open-source metadata platform for the modern data stack. Read about the architectures of different metadata systems and why DataHub excels [here](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained). Also read our
[LinkedIn Engineering blog post](https://engineering.linkedin.com/blog/2019/data-hub), check out our [Strata presentation](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019) and watch our [Crunch Conference Talk](https://www.youtube.com/watch?v=OB-O0Y6OYDE). You should also visit [DataHub Architecture](docs/architecture/architecture.md) to get a better understanding of how DataHub is implemented and [DataHub Onboarding Guide](docs/modeling/extending-the-metadata-model.md) to understand how to extend DataHub for your own use cases.

## Quickstart

Please follow the [DataHub Quickstart Guide](https://datahubproject.io/docs/quickstart) to get a copy of DataHub up & running locally using [Docker](https://docker.com). As the guide assumes some basic knowledge of Docker, we'd recommend you to go through the "Hello World" example of [A Docker Tutorial for Beginners](https://docker-curriculum.com) if Docker is completely foreign to you.

## Demo and Screenshots

There's a [hosted demo environment](https://datahubproject.io/docs/demo) where you can play around with DataHub before installing.

[![DataHub Demo GIF](docs/imgs/entity.png)](https://datahubproject.io/docs/demo)

## Source Code and Repositories

- [linkedin/datahub](https://github.com/linkedin/datahub): This repository contains the complete source code for both DataHub's frontend & backend services.
- [linkedin/datahub-gma](https://github.com/linkedin/datahub-gma): This repository contains the source code for DataHub's metadata infrastructure libraries (Generalized Metadata Architecture, or GMA).

## Documentation

We have documentation available at [https://datahubproject.io/docs/](https://datahubproject.io/docs/).

## Releases

See [Releases](https://github.com/linkedin/datahub/releases) page for more details. We follow the [SemVer Specification](https://semver.org) when versioning the releases and adopt the [Keep a Changelog convention](https://keepachangelog.com/) for the changelog format.

## Features & Roadmap

Check out DataHub's [Features](docs/features.md) & [Roadmap](docs/roadmap.md).

## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/CONTRIBUTING.md) for more details. We also have a [contrib](contrib) directory for incubating experimental features.

## Community

Join our [slack workspace](https://slack.datahubproject.io) for discussions and important announcements. You can also find out more about our upcoming [town hall meetings](docs/townhalls.md) and view past recordings.

## Adoption

Here are the companies that have officially adopted DataHub. Please feel free to add yours to the list if we missed it.

- [Banksalad](https://www.banksalad.com)
- [DefinedCrowd](http://www.definedcrowd.com)
- [DFDS](https://www.dfds.com/)
- [Expedia Group](http://expedia.com)
- [Experius](https://www.experius.nl)
- [Geotab](https://www.geotab.com)
- [Grofers](https://grofers.com)
- [Klarna](https://www.klarna.com)
- [LinkedIn](http://linkedin.com)
- [Peloton](https://www.onepeloton.com)
- [Saxo Bank](https://www.home.saxo)
- [Shanghai HuaRui Bank](https://www.shrbank.com)
- [ThoughtWorks](https://www.thoughtworks.com)
- [TypeForm](http://typeform.com)
- [Uphold](https://uphold.com)
- [Viasat](https://viasat.com)
- [Wolt](https://wolt.com)

## Select Articles & Talks

- [DataHub Project Newsletter on Medium](https://medium.com/datahub-project)
- [DataHub YouTube Channel](https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w)
- [Saxo Bank: Enabling Data Discovery in Data Mesh](https://medium.com/datahub-project/enabling-data-discovery-in-a-data-mesh-the-saxo-journey-451b06969c8f)
- Bringing The Power Of The DataHub Real-Time Metadata Graph To Everyone At Acryl Data: [Data Engineering Podcast](https://www.dataengineeringpodcast.com/acryl-data-datahub-metadata-graph-episode-230/)
- [DataHub: Popular Metadata Architectures Explained](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained)
- [Driving DataOps Culture with LinkedIn DataHub](https://www.youtube.com/watch?v=ccsIKK9nVxk) @ [DataOps Unleashed 2021](https://dataopsunleashed.com/#shirshanka-session)
- [The evolution of metadata: LinkedIn’s story](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019) @ [Strata Data Conference 2019](https://conferences.oreilly.com/strata/strata-ny-2019.html)
- [Journey of metadata at LinkedIn](https://www.youtube.com/watch?v=OB-O0Y6OYDE) @ [Crunch Data Conference 2019](https://crunchconf.com/2019)
- [DataHub Journey with Expedia Group](https://www.youtube.com/watch?v=ajcRdB22s5o)
- [Data Discoverability at SpotHero](https://www.slideshare.net/MaggieHays/data-discoverability-at-spothero)
- [Data Catalogue — Knowing your data](https://medium.com/albert-franzi/data-catalogue-knowing-your-data-15f7d0724900)
- [DataHub: A Generalized Metadata Search & Discovery Tool](https://engineering.linkedin.com/blog/2019/data-hub)
- [Open sourcing DataHub: LinkedIn’s metadata search and discovery platform](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p)
- [Emerging Architectures for Modern Data Infrastructure](https://a16z.com/2020/10/15/the-emerging-architectures-for-modern-data-infrastructure/)

See the full list [here](docs/links.md).

## License

[Apache License 2.0](./LICENSE).
