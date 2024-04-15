import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useColorMode } from "@docusaurus/theme-common";
import Link from "@docusaurus/Link";

import GuildSection from "./_components/GuildSection";

const guildSections = [
  {
    name: "Top Code Contributor",
    description: "",
    alttext: "Building high-impact features and integrations within DataHub",
    badge: "/img/guild/badge-top-code-contributor.svg",
    people: [
      {
        name: "Aezo Teo",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/aezo-teo.jpg",
        bio: (
          <>
            <b>Data Engineer, Grab</b>
            <p>
              <p> </p>
              Contributed 5 commits in 2022 to the main DataHub Project & Helm repos, including Stateful Ingestion support for Presto-on-Hive
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/aezomz",
          twitter: "https://twitter.com/morning_teofee",
          github: "https://github.com/aezomz",
        },
      },
      {
        name: "Arun Vasudevan",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/arun-vasudevan.jpg",
        bio: (
          <>
            <b>Staff Software Engineer, Peloton</b>
            <p>
              <p> </p>
              Contributed 9 commits in 2022 to the main DataHub Project, DataHub Actions; improvements to Kafka Connect
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/arun-vasudevan-55117368/",
          github: "https://github.com/arunvasudevan",
        },
      },
      {
        name: "Boyuan Zhang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/bz_white.jpg",
        bio: (
          <>
            <b>Data Engineer, Credit Karma</b>
            <p>
              <p> </p>
              Contributed 8 commits in 2022, including improvements to dbt & Kafka ingestion and support for Glue profiling
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/bbbzhang",
          },
        },
      {
        name: "Bumsoo Kim",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/bumsoo-kim.jpg",
        bio: (
          <>
            <b>Software Engineer</b>
            <p>
              <p> </p>
              Contributed 4 commits in 2022, including improvements to Airflow logging and DataHub Helm charts
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/bumsoo",
          github: "https://github.com/bskim45",
          },
        },
      {
        name: "David Haglund",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/david-haglund.jpeg",
        bio: (
          <>
            <b>Data Engineer, SSAB</b>
            <p>
              <p> </p>
              Contributed 15 commits in 2022, including improvements to DataHub Helm Charts, DataHub docs, and more
            </p>
          </>
        ),        
        social: {
          github: "https://github.com/daha",
          },
        },
      {
        name: "David Sánchez",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/david-sanchez.jpg",      
        bio: (
          <>
            <b>Principal Data Engineer, Cabify</b>
            <p>
              <p> </p>
              Contributed 7 commits in 2022, improving BigQuery and Tableau connectors and expanding MLFeatureTable functionality
            </p>
          </>
        ),        
        social: {
          },
        },
      {
        name: "Djordje Mijatovic",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/dm_white.jpg",
        bio: (
          <>
            <b>Senior Java Developer</b>
            <p>
              <p> </p>
              Contributed 6 commits in 2022, including building support for Neo4j multi-hop queries
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/djordje-mijatovic-aa22bb76/",
          },
        },
      {
        name: "Ebu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/e_white.jpg",
        bio: (
          <>
            <b>Core Staff, KDDI</b>
            <p>
              <p> </p>
              Contributed 5 commits in 2022, including a new Vertica ingestion source and animated DataHub logos
            </p>
          </>
        ),
        social: {
          github: "https://github.com/eburairu",
          },
        },
      {
        name: "Eric Ladouceur",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/el_white.jpg",
        bio: (
          <>
            <b>Technical Advisor, Canadian Centre for Cyber Security</b>
            <p>
              <p> </p>
              Contributed 6 commits in 2022, including the Iceberg ingestion source
            </p>
          </>
        ),
        social: {
          github: "https://github.com/cccs-eric",
          },
        },
      {
        name: "Felix Lüdin",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fl_white.jpg",
        bio: (
          <>
            <b>Process Consultant Business Analytics, Endress+Hauser</b>
            <p>
              <p> </p>
              Contributed 15 commits in 2022 to the main DataHub Project, DataHub Helm chart, and DataHub Actions repos
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/felix-l%C3%BCdin-222304209/",
          github: "https://github.com/Masterchen09",
          },
        },
      {
        name: "Jordan Wolinsky",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/jordan-wolinsky.jpeg",
        bio: (
          <>
            <b>Senior Software Engineer, Zephyr AI</b>
            <p>
              <p> </p>
              Contributed 5 commits in 2022, including improvements to data profiling functionality
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/jordan-wolinsky/",
          github: "https://github.com/jiafi",
          },
        },
      {
        name: "Marcin Szymanski",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/marcin-szymanski.JPG",
        bio: (
          <>
            <b>Data Engineering Manager, Esure</b>
            <p>
              <p> </p>
              Contributed 5 commits in 2022, including improvements to Trino and Unity Catalog ingestion
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/marcinszymanskipl/",
          github: "https://github.com/ms32035",
          web: "www.marcinszymanski.pl",
        },
      },
        {
        name: "Mert Tunc",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mert-tunc.png",
        bio: (
          <>
            <b>Staff Software Engineer, Udemy</b>
            <p>
              <p> </p>
              Contributed 6 commits in 2022, including improvements to Kafka and MySQL ingestion
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/merttunc96/",
          },
        },
        {
        name: "Mike Schlosser",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mike-schlosser.jpeg",
        bio: (
          <>
            <b>Lead Software Engineer, Kyros</b>
            <p>
              <p> </p>
              Contributed 6 commits in 2022, including support for Snowflake auth and fixes to Docker Compose
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/michael-schlosser",
          twitter: "https://twitter.com/Mikeschlosser16",
          github: "https://github.com/Mikeschlosser16",
          },
        },
        {
        name: "Parham Ghazanfari",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/pg_white.jpg",
        bio: (
          <>
            <b>Software Development Engineer, Amazon</b>
            <p>
              <p> </p>
              Contributed 4 commits in 2022, including support for MSK IAM authentication
            </p>
          </>
        ), 
        social: {
          linkedin: "https://www.linkedin.com/in/parham-ghazanfari-a8b40b89/",
          github: "https://github.com/pghazanfari",
          },
        },
        {
        name: "Piotr Skrydalewicz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ps_white.jpg",
        bio: (
          <>
            <b>Data Engineering Consultant</b>
            <p>
              <p> </p>
              Contributed 5 commits in 2022, including improvements to dbt and Glue ingestion sources and support for SparkSQL dialect
            </p>
          </>
        ),
        social: {
          },
        },
        {
        name: "Xu Wang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/xu-wang.jpeg",
        bio: (
          <>
            <b>Staff Software Engineer, Included Health</b>
            <p>
              <p> </p>
              Contributed 6 commits in 2022, including metadata model changes to support Notebook entities
            </p>
          </>
        ),
        social: {
          },
        },
    ],
  },
  {
    name: "Community Supporter",
    description:  "",
    alttext: "Helping Community Members succeed in implementing DataHub", 
    badge: "/img/guild/badge-community-supporter.svg",
    people: [
      {
        name: "Mohamad Amzar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amz.jpeg",
        bio: (
          <>
            <b>Analytics Engineer, CDX</b>
            <p>
              <p> </p>
              Consistently provides guidance to others in the #troubleshoot and #ingestion Slack channels
            </p>
          </>
        ),
        social: {
          github: "https://github.com/amzar96",
        },
      },
      {
        name: "Nguyen Tri Hieu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/hieu-nguyen-tri-hieu.jpg",
        bio: (
          <>
            <b>Data Engineer, Fossil Vietnam</b>
            <p>
              <p> </p>
              Regularly helps others across all support channels in Slack
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/hieunguyen-it/",
        },
      },
      {
        name: "Nicholas Shook",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ns_yellow.jpg",
        bio: (
          <>
            <b>Platform Engineer</b>
            <p>
              <p> </p>
              Frequently helps others navigate implementation questions in the #all-things-deployment and #troubleshoot Slack channels
            </p>
          </>
        ),
        social: {
          web: "HTTPS://shook.family",
        },
      },
      {
        name: "Pablo Ochoa",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/pablo-ochoa-ajamil.jpg",
        bio: (
          <>
            <b>Big Data and Data Governance Consultant, Graphenus</b>
            <p>
              <p> </p>
              Consistently jumps in to address questions in the #troubleshoot and #all-things-deployment Slack channels
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/pablo-ochoa-ajamil-61037a211/",
        },
      },
      {
        name: "Patrick Braz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/patrick-franco-braz.jpeg",
        bio: (
          <>
            <b>Data Engineering, Hurb</b>
            <p>
              <p> </p>
              Regularly provides guidance to Community Members in the #troubleshoot and #ingestion Slack channels
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/patrick-franco-braz/",
          github: "https://github.com/PatrickfBraz",
        },
      },
      {
        name: "Pedro Aguiar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/pa_yellow.jpg",
        bio: (
          <>
            <b>Data Analyst</b>
            <p>
              <p> </p>
              Routinely provides helpful support to others in the #troubleshoot Slack channel
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/pdraguiar",
          github: "https://github.com/pdraguiar",
          web: "http://www.pedroaguiar.com.br/",
        },
      },
      {
        name: "Steve Pham",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/cuong-pham.jpeg",
        bio: (
          <>
            <b>Principal Engineer</b>
            <p>
              <p> </p>
              Reliably provides direction to Community Members across all support channels in Slack
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/steve-pham",
        },
      },
      {
        name: "Xianglong LIU",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/xl_yellow.jpg",
        bio: (
          <>
            <b>Data Platform Engineer</b>
            <p>
              <p> </p>
              Continually goes above and beyond to answer any and all questions from Community Members across all support in Slack
            </p>
          </>
        ),
        social: {
        },
      },
    ],
  },
  {
    name: "DataHub Champion",
    description:  "",
    alttext: "Amplifying awareness and adoption of DataHub",
    badge: "/img/guild/badge-datahub-champion.svg",
    people: [
      {
        name: "Abhishek Sharma",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/as_black.jpg",
        bio: (
          <>
            <b>Software Engineer</b>
            <p>
              <p> </p>
              Driving adoption of DataHub at his organization and featured in our inaugural <a href="https://blog.datahubproject.io/humans-of-datahub-a6ef2a5a1719" target="_blank"> Humans of DataHub</a> post
            </p>
          </>
        ),
        social: {
        },
      },
      {
        name: "Alexander Dobrev",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/alex-dobrev.jpg",
        bio: (
          <>
            <b>Product Manager, Grab</b>
            <p>
              <p> </p>
              Shared Grab's expereince adopting and implementing DataHub during <a href="https://www.youtube.com/watch?v=30kOI8ziewU" target="_blank"> October 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/adobrev/",
        },
      },
      {
        name: "Amanda Ng",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amanda-ng.png",
        bio: (
          <>
            <b>Senior Software Engineer, Grab</b>
            <p>
              <p> </p>
              Shared Grab's expereince adopting and implementing DataHub during <a href="https://www.youtube.com/watch?v=30kOI8ziewU" target="_blank"> October 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://sg.linkedin.com/in/amandang19",
          github: "https://github.com/ngamanda",
          web: "https://ngamanda.com",
        },
      },
      {
        name: "Atul Saurav",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/atul-saurav.png",
        bio: (
          <>
            <b>Data Governance Architect</b>
            <p>
              <p> </p>
              Demoed custom desktop app to search for Glossary Terms outside of DataHub during <a href="https://www.youtube.com/watch?v=rPPFygMPCNU" target="_blank"> August 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://linkedIn.com/in/atulsaurav",
          twitter: "https://twitter.com/twtAtul",
        },
      },
      {
        name: "Divya Manohar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/divya-manohar.png",      
        bio: (
          <>
            <b>Software Engineer, Stripe</b>
            <p>
              <p> </p>
              Shared an in-depth look into how The Stripe Team <a href="https://blog.datahubproject.io/the-stripe-story-beating-data-pipeline-observability-and-timeliness-concerns-with-datahub-8ba42788aca2" target="_blank"> addressed data pipeline observability and timeliness concerns</a> with DataHub
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/divya-manohar-20862716a/",
        },
      },
      {
        name: "Edward Vaisman",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/edward-vaisman.png",
        bio: (
          <>
            <b>Staff Customer Innovation Engineer, Confluent</b>
            <p>
              <p> </p>
              Demoed his contribution to define lineage relationships via YAML during <a href="https://www.youtube.com/watch?v=-25SwzwkISM&t=1s" target="_blank"> February 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/edwardvaisman",
          github: "https://github.com/eddyv",
          web: "https://edwardvaisman.ca",
        },
      },
      {
        name: "Eric Cooklin",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ec_black.jpg",
        bio: (
          <>
            <b>Sr Data Engineer, Stash</b>
            <p>
              <p> </p>
              Shared his experience contributing to the DataHub project during <a href="https://www.youtube.com/watch?v=KCh7kwdvPwE" target="_blank"> January 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/eric-cooklin-3b63a1129",
        },
      },
      {
        name: "Fredrik Sannholm",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fs_black.jpg",
        bio: (
          <>
            <b>Staff Engineer</b>
            <p>
              <p> </p>
              Driving DataHub adoption at Wolt and featured in <a href="https://blog.datahubproject.io/humans-of-datahub-fredrik-sannholm-d673b1877f2b" target="_blank"> Humans of DataHub</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/fredriksannholm",
        },
      },
      {
        name: "Gary Stafford",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/gary-stafford.JPG",
        bio: (
          <>
            <b>Principal Solutions Architect/Analytics TFC, AWS</b>
            <p>
              <p> </p>
              Wrote an in-depth overview of DataHub's features and implementation strategies <a href="https://garystafford.medium.com/end-to-end-data-discovery-observability-and-governance-on-aws-with-linkedins-datahub-8c69e7e8c925" target="_blank"> 
                on Medium</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/garystafford/",
          twitter: "https://twitter.com/GaryStafford",
          github: "https://github.com/garystafford",
          web: "https://medium.com/@GaryStafford",
        },
      },
      {
        name: "Harvey Li",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/hl_black.jpg",
        bio: (
          <>
            <b>Lead Data Engineer, Grab</b>
            <p>
              <p> </p>
              Shared Grab's expereince adopting and implementing DataHub during <a href="https://www.youtube.com/watch?v=30kOI8ziewU" target="_blank"> October 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/li-haihui",
          github: "https://github.com/HarveyLeo",
        },
      },
      {
        name: "Hyejin Yoon",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/hyejin-yoon.jpg",
        bio: (
          <>
            <b>Data Engineer, SOCAR</b>
            <p>
              <p> </p>
              Driving DataHub adoption at SOCAR, is a strong advocate for the DataHub Community, and featured in <a href="https://blog.datahubproject.io/humans-of-datahub-hyejin-yoon-4e546696cee8" target="_blank"> Humans of DataHub</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/hyejinyoon/",
        },
      },
      {
        name: "Imane Lafnoune",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/imane-lafnoune.jpeg",
        bio: (
          <>
            <b>Data Engineer, Sicara</b>
            <p>
              <p> </p>
              Published a 5-min guide to <a href="https://www.sicara.fr/blog-technique/databricks-data-catalog-with-datahub" target="_blank"> integrate DataHub and Databricks</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/imanelafnoune/",
          twitter: "https://twitter.com/ImaneLafn",
          github: "https://github.com/imlaf",
        },
      },
      {
        name: "Kartik Darapuneni",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/kd_black.jpg",
        bio: (
          <>
            <b>Software Engineer, Included Health</b>
            <p>
              <p> </p>
              Demoed custom work to embed Looker resources in DataHub during <a href="https://www.youtube.com/watch?v=33hxlg4YgCQ&t=2s" target="_blank"> April 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
        },
      },

      {
        name: "Liangjun Jiang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/lj_black.jpg",
        bio: (
          <>
            <b>Software Engineering Manager</b>
            <p>
              <p> </p>
              Published a DataHub deployment guide <a href="https://liangjunjiang.medium.com/deploy-open-source-datahub-fd597104512b" target="_blank"> on Medium</a>
            </p>
          </>
        ),
        social: {
        },
      },
      {
        name: "Mike Linthe",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mike-linthe.jpeg",
        bio: (
          <>
            <b>COO, Contiamo</b>
            <p>
              <p> </p>
              Strong advocate for adopting DataHub and featured in <a href="https://blog.datahubproject.io/humans-of-datahub-mike-linthe-f8d4c205983d" target="_blank"> Humans of DataHub</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/mike-linthe/",
          web: "www.contiamo.com",
        },
      },
      {
        name: "Nidhin Nandhakumar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/nidhin-nandhakumar.jpeg",
        bio: (
          <>
            <b>Senior Data Engineer, Coursera</b>
            <p>
              <p> </p>
              Driving DataHub adoption at Coursera
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/nidhin-nandhakumar-874ba885/",
        },
      },
      {
        name: "Sergio Gómez",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/sergio-gomez-villamor.jpeg",
        bio: (
          <>
            <b>Technical Lead, Adevinta</b>
            <p>
              <p> </p>
              Driving DataHub adoption at Adevinta and featured in <a href="https://blog.datahubproject.io/humans-of-datahub-sergio-gomez-villamor-bd2ecfb5ccc4" target="_blank"> Humans of DataHub</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/sgomezvillamor/",
          twitter: "https://twitter.com/sgomezvillamor",
          github: "https://github.com/sgomezvillamor",
        },
      },
      {
        name: "Steven Po",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/steven-zhi-wai-po.jpeg",
        bio: (
          <>
            <b>Senior Data Engineer, Coursera</b>
            <p>
              <p> </p>
              Driving DataHub adoption at Coursera and featured in <a href="https://blog.datahubproject.io/humans-of-datahub-steven-po-b56e7bf8cd6b" target="_blank"> Humans of DataHub</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/steven-zhi-wai-po-12a28626/",
        },
      },
      {
        name: "Vishal Shah",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/vishal-shah.jpg",
        bio: (
          <>
            <b>Senior Software Engineer, Etsy</b>
            <p>
              <p> </p>
              Shared Etsy's experience adopting and deploying DataHub at <a href="https://www.youtube.com/watch?v=kLe_xfTR_rM" target="_blank"> August 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/vishal-c-shah",
        },
      },
      {
        name: "Zhong Xu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/zx_black.jpg",
        bio: (
          <>
            <b>Software Engineer, Pinterest</b>
            <p>
              <p> </p>
              Shared Pinterest's journey adopting and deploying DataHub at <a href="https://www.youtube.com/watch?v=YoxTg8tQSwg" target="_blank"> December 2022 Town Hall</a>
            </p>
          </>
        ),
        social: {
        },
      },
    ],
  },
];

const HeroImage = (props) => {
  const { colorMode } = useColorMode();
  return (
    <>
      <img style={{ marginBottom: "2rem" }} src={`/img/guild/guild-logo-${colorMode}.svg`} alt="DataHub Data Practitioners Guild" {...props} />
    </>
  );
};

function Guild() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <HeroImage />
              <p className="hero__subtitle">
                Celebrating community members that have gone above and beyond to contribute to the collective success of DataHub
              </p>
            </div>
            <div style={{ textAlign: "right" }}>
              <Link className="button button--secondary button--md" to="/champions">
                See the DataHub Champions (2023) →
              </Link>
            </div>
          </div>
          {guildSections.map((section, idx) => (
            <GuildSection key={idx} {...section} />
          ))}
        </div>
      </header>
    </Layout>
  );
}

export default Guild;
