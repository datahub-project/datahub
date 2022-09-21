import React from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import CodeBlock from "@theme/CodeBlock";

import Hero from "./_components/Hero";
import Features from "./_components/Features";
import Quotes from "./_components/Quotes";
import { Section, PromoSection } from "./_components/Section";
import { PlatformLogos, CompanyLogos } from "./_components/Logos";
import RoundedImage from "./_components/RoundedImage";

const example_recipe = `
source:
  type: "mysql"
  config:
    username: "datahub"
    password: "datahub"
    host_port: "localhost:3306"
sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'`.trim();
const example_recipe_run = "datahub ingest -c recipe.yml";

function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  if (siteConfig.customFields.isSaas) {
    window.location.replace("/docs");
  }

  return !siteConfig.customFields.isSaas ? (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <Hero />
      <Features />
      <PlatformLogos />
      <Section title="A Modern Approach to Metadata Management" withBackground>
        <div className="container">
          <div className="row row--padded row--centered">
            <div className="col col--5">
              <h2>Automated Metadata Ingestion</h2>
              <p>
                <b>Push</b>-based ingestion can use a prebuilt emitter or can
                emit custom events using our framework.
              </p>
              <p>
                <b>Pull</b>-based ingestion crawls a metadata source. We have
                prebuilt integrations with Kafka, MySQL, MS SQL, Postgres, LDAP,
                Snowflake, Hive, BigQuery, and more. Ingestion can be automated
                using our Airflow integration or another scheduler of choice.
              </p>
              <p>
                Learn more about metadata ingestion with DataHub in the{" "}
                <Link to={"docs/metadata-ingestion"}>docs</Link>.
              </p>
            </div>
            <div className="col col--6 col--offset-1">
              <div>
                <div>
                  <CodeBlock
                    className={"language-yml"}
                    metastring='title="recipe.yml"'
                  >
                    {example_recipe}
                  </CodeBlock>
                </div>
                <div>
                  <CodeBlock className={"language-shell"}>
                    {example_recipe_run}
                  </CodeBlock>
                </div>
              </div>
            </div>
          </div>

          <div className="row row--padded row--centered">
            <div className="col col--6">
              <RoundedImage
                img={require("/img/screenshots/lineage.png")}
                alt="DataHub Lineage Screenshot"
              />
            </div>
            <div className="col col--5 col--offset-1">
              <h2>
                <span>Discover Trusted Data</span>
              </h2>
              <p>
                Browse and search over a continuously updated catalog of
                datasets, dashboards, charts, ML models, and more.
              </p>
            </div>
          </div>

          <div className="row row--padded row--centered">
            <div className="col col--5">
              <h2>
                <span>Understand Data in Context</span>
              </h2>
              <p>
                DataHub is the one-stop shop for documentation, schemas,
                ownership, lineage, pipelines, data quality, usage information,
                and more.
              </p>
            </div>
            <div className="col col--6 col--offset-1">
              <RoundedImage
                img={require("/img/screenshots/metadata.png")}
                alt="DataHub Metadata Screenshot"
              />
            </div>
          </div>
        </div>
      </Section>
      <Section title="Trusted Across the Industry">
        <CompanyLogos />
        <Quotes />
      </Section>
    </Layout>
  ) : null;
}

export default Home;
