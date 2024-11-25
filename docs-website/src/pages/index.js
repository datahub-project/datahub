import React, { useState } from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import CodeBlock from "@theme/CodeBlock";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Hero from "./_components/Hero";
import Features from "./_components/Features";
import { Section, PromoSection } from "./_components/Section";
import { PlatformLogos } from "./_components/Logos";
import RoundedImage from "./_components/RoundedImage";
import { CompanyLogos } from "./_components/Logos";
import QuickstartContent from "./_components/QuickstartContent";
import Testimonials from "./_components/Testimonials";
import Ecosystem from "./_components/Ecosystem";
import Community from "./_components/Community";
import SocialMedia from "./_components/SocialMedia";
import CaseStudy from "./_components/CaseStudy";
import Trial from "./_components/Trial";
import CloseButton from "@ant-design/icons/CloseCircleFilled";

const companyIndexes = require("../../adoptionStoriesIndexes.json");
const companies = companyIndexes.companies;
const keyCompanySlugs = ["netflix", "pinterest", "notion", "snap", "optum"]; //, "airtel"];
const keyCompanies = keyCompanySlugs
  .map((slug) => companies.find((co) => co.slug === slug))
  .filter((isDefined) => isDefined);

function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  if (siteConfig.customFields.isSaas) {
    window.location.replace("/docs");
  }

  const [isTourModalVisible, setIsTourModalVisible] = useState(false);
  const onOpenTourModal = () => {
    setIsTourModalVisible(true);
  };
  const onCloseTourModal = () => {
    setIsTourModalVisible(false);
  };
  return !siteConfig.customFields.isSaas ? (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a metadata management platform, spaning data discovery, observability and governance. It helps you tame the complexity of diverse data ecosystems."
    >
      {isTourModalVisible ? (
        <div className="tourModal">
          <div className="closeButtonWrapper" onClick={onCloseTourModal}>
            <CloseButton />
          </div>
          <iframe src="https://www.acryldata.io/tour" />
        </div>
      ) : null}
      <Hero onOpenTourModal={onOpenTourModal} />
      <div className="comapny__logos">
        <div className="text">
          Trusted by industry leaders&nbsp;
          <br />
          around the world.
        </div>
        <div className="company_logos_list_wrapper">
          {keyCompanies.map((company) => (
            <a
              href={
                company.slug != "snap"
                  ? `/adoption-stories#${company.slug}`
                  : undefined
              }
            >
              <img
                src={useBaseUrl(company.imageUrl)}
                alt={company.name}
                title={company.name}
                className={"company_logo"}
              />
            </a>
          ))}
          <a href="/adoption-stories" class="more_link">
            + More
          </a>
        </div>
        {/* <div style={{ textAlign: "center", margin: "1rem" }}>
          <Link
            className="button button--secondary button--md"
            to={useBaseUrl("adoption-stories")}
          >
            Check Out Adoption Stories →
          </Link>
        </div> */}
      </div>
      <QuickstartContent />
      <Testimonials />
      {/* <Features /> */}
      <Ecosystem />
      <Community />
      <SocialMedia />
      <CaseStudy />
      <Trial onOpenTourModal={onOpenTourModal} />
      {/* <Section>
        <div className="container">
          <div className="row row--centered">
            <div className="col col--6">
              <div>
                <iframe
                  style={{
                    display: "block",
                    margin: "0 auto",
                    borderRadius: "0.4rem",
                  }}
                  width="620"
                  height="320"
                  src="https://www.youtube.com/embed/oxNzH40m5E0"
                  frameBorder="0"
                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                  allowFullScreen
                  title="Intro Video"
                />
              </div>
            </div>
            <div className="col col--5 col--offset-1">
              <h1
                style={{
                  width: "18rem",
                }}
              >
                The Origins of DataHub
              </h1>
              <hr style={{ border: "2px solid black", width: "20rem" }}></hr>
              <p style={{ fontSize: "18px" }}>
                Explore DataHub's journey from search and data discovery tool at
                LinkedIn to the #1 open source metadata management platform,
                through the lens of its founder and some amazing community
                members.
              </p>
            </div>
          </div>
        </div>
      </Section>
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
                ownership, data lineage, pipelines, data quality, usage
                information, and more.
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
      </Section> */}
    </Layout>
  ) : null;
}

export default Home;
