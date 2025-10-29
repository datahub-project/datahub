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
<a href="https://datahub.com">
<img alt="DataHub" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg" height="150" />
</a>
</p>
<!-- -->

# DataHub: The Data Discovery Platform for the Modern Data Stack

### Built with ❤️ by <img src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg" width="20"/> [DataHub](https://datahub.com) and <img src="https://docs.datahub.com/img/LI-In-Bug.png" width="20"/> [LinkedIn](https://engineering.linkedin.com)

<div>
  <a target="_blank" href="https://github.com/datahub-project/datahub/blob/master/LICENSE">
    <img alt="Apache 2.0 License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg?label=license&labelColor=133554&color=1890ff" /></a>
  <a target="_blank" href="https://pypi.org/project/acryl-datahub/">
    <img alt="PyPI" src="https://img.shields.io/pypi/dm/acryl-datahub?label=downloads&labelColor=133554&color=1890ff" /></a>
  <a target="_blank" href="https://github.com/datahub-project/datahub/pulse">
    <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/datahub-project/datahub?label=commits&labelColor=133554&color=1890ff" /></a>
  <br />
  <a target="_blank" href="https://datahub.com/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme">
    <img alt="Slack" src="https://img.shields.io/badge/slack-join_community-red.svg?logo=slack&labelColor=133554&color=1890ff" /></a>
  <a href="https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w">
    <img alt="YouTube" src="https://img.shields.io/youtube/channel/subscribers/UC3qFQC5IiwR5fvWEqi_tJ5w?style=flat&logo=youtube&label=subscribers&labelColor=133554&color=1890ff"/></a>
  <a href="https://medium.com/datahub-project/">
    <img alt="Medium" src="https://img.shields.io/badge/blog-DataHub-red.svg?style=flat&logo=medium&logoColor=white&labelColor=133554&color=1890ff" /></a>
  <a href="https://x.com/datahubproject">
    <img alt="X (formerly Twitter) Follow" src="https://img.shields.io/badge/follow-datahubproject-red.svg?style=flat&logo=x&labelColor=133554&color=1890ff" /></a>
</div>

---

### 🏠 Docs: [docs.datahub.com](https://docs.datahub.com/)

[Quickstart](https://docs.datahub.com/docs/quickstart) |
[Features](https://docs.datahub.com/docs/features) |
[Adoption](https://datahub.com/resources/?2004611554=dh-stories) |
[Demo](https://demo.datahub.com/) |
[Town Hall](https://docs.datahub.com/docs/townhalls)


## Introduction

DataHub is an open-source data catalog for the modern data stack. Read about the architectures of different metadata systems and why DataHub excels [here](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained). Also read our
[LinkedIn Engineering blog post](https://engineering.linkedin.com/blog/2019/data-hub), check out our [Strata presentation](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019) and watch our [Crunch Conference Talk](https://www.youtube.com/watch?v=OB-O0Y6OYDE). You should also visit [DataHub Architecture](docs/architecture/architecture.md) to get a better understanding of how DataHub is implemented.


There's a [hosted demo environment](https://demo.datahub.com/) courtesy of DataHub where you can explore DataHub without installing it locally.

## Quickstart

```
python3 -m pip install --upgrade acryl-datahub
datahub docker quickstart
```

Please follow the [DataHub Quickstart Guide](https://docs.datahub.com/docs/quickstart) to run DataHub locally using [Docker](https://docker.com).

## Community

Join our [Slack workspace](https://datahub.com/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme) for discussions and important announcements. You can also find out more about our upcoming [town hall meetings](docs/townhalls.md) and view past recordings.


## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/CONTRIBUTING.md) for more details. We also have a [contrib](contrib) directory for incubating experimental features.

If you're looking to build & modify datahub please take a look at our [Development Guide](https://docs.datahub.com/docs/developers).


## Adoption

[Here are the companies](https://datahub.com/resources/?2004611554=dh-stories) that have officially adopted DataHub. Please feel free to add yours to the list if we missed it.


## DataHub Cloud

* [Why DataHub Cloud](https://datahub.com/products/why-datahub-cloud/)
* [DataHub Cloud vs DataHub Core](https://datahub.com/products/cloud-vs-core/)



## Source Code and Repositories

- [datahub-project/datahub](https://github.com/datahub-project/datahub): This repository contains the complete source code for DataHub's metadata model, metadata services, integration connectors and the web application.
- [acryldata/datahub-actions](https://github.com/acryldata/datahub-actions): DataHub Actions is a framework for responding to changes to your DataHub Metadata Graph in real time.
- [acryldata/datahub-helm](https://github.com/acryldata/datahub-helm): Helm charts for deploying DataHub on a Kubernetes cluster
- [acryldata/meta-world](https://github.com/acryldata/meta-world): A repository to store recipes, custom sources, transformations and other things to make your DataHub experience magical.
- [dbt-impact-action](https://github.com/acryldata/dbt-impact-action): A github action for commenting on your PRs with a summary of the impact of changes within a dbt project.
- [datahub-tools](https://github.com/makenotion/datahub-tools): Additional python tools to interact with the DataHub GraphQL endpoints, built by Notion.
- [business-glossary-sync-action](https://github.com/acryldata/business-glossary-sync-action): A github action that opens PRs to update your business glossary yaml file.
- [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub): A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for DataHub.



## License

[Apache License 2.0](./LICENSE).
