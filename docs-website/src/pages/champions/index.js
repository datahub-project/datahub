import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useColorMode } from "@docusaurus/theme-common";
import Link from "@docusaurus/Link";

import ChampionSection from "./_components/ChampionSection";

const championSections = [
  {
    people: [
      {
        name: "Piotr Skrydalewicz",
        position: "Data Engineer, Adevinta",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/aezo-teo.jpg",
        bio: (
          <>
            <p>
              Contributed 5 commits in 2022 to the main DataHub Project & Helm repos, including Stateful Ingestion support for Presto-on-Hive
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/aezomz",
          twitter: "https://twitter.com/morning_teofee",
          github: "https://github.com/aezomz",
        },
        location: "Barcelona, Spain"
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
];

const HeroImage = (props) => {
  const { colorMode } = useColorMode();
  return (
    <>
      <img style={{ marginBottom: "2rem", height: "14rem" }} src={`/img/champions/champions-logo-${colorMode}.png`} alt="DataHub Champions" {...props} />
    </>
  );
};

function Champion() {
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
              <h1>DataHub Champions</h1>
              <p className="hero__subtitle">
                Recognizing community members who have made exceptional contributions to further the collective success of DataHub.              
              </p>
            </div>
            <h1>Meet Our Champions</h1>
            <div style={{ textAlign: "right" }}>
              <Link className="button button--secondary button--md" to="/guild">
                See Data Practitioner Guild (2022) →
              </Link>
            </div>
          </div>
          
          {championSections.map((section, idx) => (
            <ChampionSection key={idx} {...section} />
          ))}
        </div>
      </header>
    </Layout>
  );
}

export default Champion;
