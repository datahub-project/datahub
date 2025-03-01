<!--HOSTED_DOCS_ONLY
import useBaseUrl from '@docusaurus/useBaseUrl';

export const Logo = (props) => {
  return (
    <div style={{ display: "flex", justifyContent: "center", padding: "20px", height: "190px" }}>
      <img
        alt="DataHub Logo"
        src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg"
        {...props}
      />
    </div>
  );
};

<Logo />

<!--
HOSTED_DOCS_ONLY-->
<p align="center">
<a href="https://datahubproject.io">
<img alt="DataHub" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg" height="150" />
</a>
</p>
<!-- -->

# DataHub: The Data Discovery Platform for the Modern Data Stack

### Built with ❤️ by <img src="https://datahubproject.io/img/acryl-logo-light-mark.png" width="20"/> [Acryl Data](https://acryldata.io) and <img src="https://datahubproject.io/img/LI-In-Bug.png" width="20"/> [LinkedIn](https://engineering.linkedin.com)

<div>
  <a target="_blank" href="https://github.com/datahub-project/datahub/blob/master/LICENSE">
    <img alt="Apache 2.0 License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg?label=license&labelColor=133554&color=1890ff" /></a>
  <a target="_blank" href="https://pypi.org/project/acryl-datahub/">
    <img alt="PyPI" src="https://img.shields.io/pypi/dm/acryl-datahub?label=downloads&labelColor=133554&color=1890ff" /></a>
  <a target="_blank" href="https://github.com/datahub-project/datahub/pulse">
    <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/datahub-project/datahub?label=commits&labelColor=133554&color=1890ff" /></a>
  <br />
  <a target="_blank" href="https://pages.acryl.io/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme">
    <img alt="Slack" src="https://img.shields.io/badge/slack-join_community-red.svg?logo=slack&labelColor=133554&color=1890ff" /></a>
  <a href="https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w">
    <img alt="YouTube" src="https://img.shields.io/youtube/channel/subscribers/UC3qFQC5IiwR5fvWEqi_tJ5w?style=flat&logo=youtube&label=subscribers&labelColor=133554&color=1890ff"/></a>
  <a href="https://blog.datahubproject.io/">
    <img alt="Medium" src="https://img.shields.io/badge/blog-DataHub-red.svg?style=flat&logo=medium&logoColor=white&labelColor=133554&color=1890ff" /></a>
  <a href="https://x.com/datahubproject">
    <img alt="X (formerly Twitter) Follow" src="https://img.shields.io/badge/follow-datahubproject-red.svg?style=flat&logo=x&labelColor=133554&color=1890ff" /></a>
</div>

---

### 🏠 Docs: [datahubproject.io](https://datahubproject.io/docs)

[Quickstart](https://datahubproject.io/docs/quickstart) |
[Features](https://datahubproject.io/docs/) |
[Roadmap](https://feature-requests.datahubproject.io/roadmap) |
[Adoption](#adoption) |
[Demo](https://demo.datahubproject.io/) |
[Town Hall](https://datahubproject.io/docs/townhalls)

---

> 📣 DataHub Town Hall is the 4th Thursday at 9am US PT of every month - [add it to your calendar!](https://rsvp.datahubproject.io/)
>
> - Town-hall Zoom link: [zoom.datahubproject.io](https://zoom.datahubproject.io)
> - [Meeting details](docs/townhalls.md) & [past recordings](docs/townhall-history.md)

> ✨ DataHub Community Highlights:
>
> - Read our Monthly Project Updates [here](https://blog.datahubproject.io/tagged/project-updates).
> - Bringing The Power Of The DataHub Real-Time Metadata Graph To Everyone At Acryl Data: [Data Engineering Podcast](https://www.dataengineeringpodcast.com/acryl-data-datahub-metadata-graph-episode-230/)
> - Check out our most-read blog post, [DataHub: Popular Metadata Architectures Explained](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained) @ LinkedIn Engineering Blog.
> - Join us on [Slack](docs/slack.md)! Ask questions and keep up with the latest announcements.

## Introduction

DataHub is an open-source data catalog for the modern data stack. Read about the architectures of different metadata systems and why DataHub excels [here](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained). Also read our
[LinkedIn Engineering blog post](https://engineering.linkedin.com/blog/2019/data-hub), check out our [Strata presentation](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019) and watch our [Crunch Conference Talk](https://www.youtube.com/watch?v=OB-O0Y6OYDE). You should also visit [DataHub Architecture](docs/architecture/architecture.md) to get a better understanding of how DataHub is implemented.

## Features & Roadmap

Check out DataHub's [Features](docs/features.md) & [Roadmap](https://feature-requests.datahubproject.io/roadmap).

## Demo and Screenshots

There's a [hosted demo environment](https://demo.datahubproject.io/) courtesy of [Acryl Data](https://acryldata.io) where you can explore DataHub without installing it locally.

## Quickstart

Please follow the [DataHub Quickstart Guide](https://datahubproject.io/docs/quickstart) to run DataHub locally using [Docker](https://docker.com).

## Development

If you're looking to build & modify datahub please take a look at our [Development Guide](https://datahubproject.io/docs/developers).

<p align="center">
<a href="https://demo.datahubproject.io/">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/entity.png"/>
</a>
</p>

## Source Code and Repositories

- [datahub-project/datahub](https://github.com/datahub-project/datahub): This repository contains the complete source code for DataHub's metadata model, metadata services, integration connectors and the web application.
- [acryldata/datahub-actions](https://github.com/acryldata/datahub-actions): DataHub Actions is a framework for responding to changes to your DataHub Metadata Graph in real time.
- [acryldata/datahub-helm](https://github.com/acryldata/datahub-helm): Repository of helm charts for deploying DataHub on a Kubernetes cluster
- [acryldata/meta-world](https://github.com/acryldata/meta-world): A repository to store recipes, custom sources, transformations and other things to make your DataHub experience magical
- [dbt-impact-action](https://github.com/acryldata/dbt-impact-action) : This repository contains a github action for commenting on your PRs with a summary of the impact of changes within a dbt project
- [datahub-tools](https://github.com/makenotion/datahub-tools) : Additional python tools to interact with the DataHub GraphQL endpoints, built by Notion
- [business-glossary-sync-action](https://github.com/acryldata/business-glossary-sync-action) : This repository contains a github action that opens PRs to update your business glossary yaml file.

## Releases

See [Releases](https://github.com/datahub-project/datahub/releases) page for more details. We follow the [SemVer Specification](https://semver.org) when versioning the releases and adopt the [Keep a Changelog convention](https://keepachangelog.com/) for the changelog format.

## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/CONTRIBUTING.md) for more details. We also have a [contrib](contrib) directory for incubating experimental features.

## Community

Join our [Slack workspace](https://pages.acryl.io/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme) for discussions and important announcements. You can also find out more about our upcoming [town hall meetings](docs/townhalls.md) and view past recordings.

## Security

See [Security Stance](docs/SECURITY_STANCE.md) for information on DataHub's Security.

## Adoption

Here are the companies that have officially adopted DataHub. Please feel free to add yours to the list if we missed it.

- [ABLY](https://ably.team/)
- [Adevinta](https://www.adevinta.com/)
- [Banksalad](https://www.banksalad.com)
- [Cabify](https://cabify.tech/)
- [ClassDojo](https://www.classdojo.com/)
- [Coursera](https://www.coursera.org/)
- [CVS Health](https://www.cvshealth.com/)
- [DefinedCrowd](http://www.definedcrowd.com)
- [DFDS](https://www.dfds.com/)
- [Digital Turbine](https://www.digitalturbine.com/)
- [Expedia Group](http://expedia.com)
- [Experius](https://www.experius.nl)
- [Geotab](https://www.geotab.com)
- [Grofers](https://grofers.com)
- [Haibo Technology](https://www.botech.com.cn)
- [hipages](https://hipages.com.au/)
- [inovex](https://www.inovex.de/)
- [Inter&Co](https://inter.co/)
- [IOMED](https://iomed.health)
- [Klarna](https://www.klarna.com)
- [LinkedIn](http://linkedin.com)
- [Moloco](https://www.moloco.com/en)
- [N26](https://n26brasil.com/)
- [Optum](https://www.optum.com/)
- [Peloton](https://www.onepeloton.com)
- [PITS Global Data Recovery Services](https://www.pitsdatarecovery.net/)
- [Razer](https://www.razer.com)
- [Rippling](https://www.rippling.com/)
- [Showroomprive](https://www.showroomprive.com/)
- [SpotHero](https://spothero.com)
- [Stash](https://www.stash.com)
- [Shanghai HuaRui Bank](https://www.shrbank.com)
- [s7 Airlines](https://www.s7.ru/)
- [ThoughtWorks](https://www.thoughtworks.com)
- [TypeForm](http://typeform.com)
- [Udemy](https://www.udemy.com/)
- [Uphold](https://uphold.com)
- [Viasat](https://viasat.com)
- [Wealthsimple](https://www.wealthsimple.com)
- [Wikimedia](https://www.wikimedia.org)
- [Wolt](https://wolt.com)
- [Zynga](https://www.zynga.com)

## Select Articles & Talks

- [DataHub Blog](https://blog.datahubproject.io/)
- [DataHub YouTube Channel](https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w)
- [Optum: Data Mesh via DataHub](https://opensource.optum.com/blog/2022/03/23/data-mesh-via-datahub)
- [Saxo Bank: Enabling Data Discovery in Data Mesh](https://medium.com/datahub-project/enabling-data-discovery-in-a-data-mesh-the-saxo-journey-451b06969c8f)
- [Bringing The Power Of The DataHub Real-Time Metadata Graph To Everyone At Acryl Data](https://www.dataengineeringpodcast.com/acryl-data-datahub-metadata-graph-episode-230/)
- [DataHub: Popular Metadata Architectures Explained](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained)
- [Driving DataOps Culture with LinkedIn DataHub](https://www.youtube.com/watch?v=ccsIKK9nVxk) @ [DataOps Unleashed 2021](https://dataopsunleashed.com/#shirshanka-session)
- [The evolution of metadata: LinkedIn’s story](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019) @ [Strata Data Conference 2019](https://conferences.oreilly.com/strata/strata-ny-2019.html)
- [Journey of metadata at LinkedIn](https://www.youtube.com/watch?v=OB-O0Y6OYDE) @ [Crunch Data Conference 2019](https://crunchconf.com/2019)
- [DataHub Journey with Expedia Group](https://www.youtube.com/watch?v=ajcRdB22s5o)
- [Data Discoverability at SpotHero](https://www.slideshare.net/MaggieHays/data-discoverability-at-spothero)
- [Data Catalogue — Knowing your data](https://medium.com/albert-franzi/data-catalogue-knowing-your-data-15f7d0724900)
- [DataHub: A Generalized Metadata Search & Discovery Tool](https://engineering.linkedin.com/blog/2019/data-hub)
- [Open sourcing DataHub: LinkedIn’s metadata search and discovery platform](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p)
- [Emerging Architectures for Modern Data Infrastructure](https://future.com/emerging-architectures-for-modern-data-infrastructure-2020/)

See the full list [here](docs/links.md).

## License

[Apache License 2.0](./LICENSE).
