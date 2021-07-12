import React from "react";
import clsx from "clsx";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./styles.module.css";

import Slider from "react-slick";
import "slick-carousel/slick/slick.css";
import "./slick-theme.css";

import YouTube from "react-youtube";

import Image from "@theme/IdealImage";
import CodeBlock from "@theme/CodeBlock";
// logos for companies using
import LogoLinkedin from "./logos/linkedin.svg";
import LogoExpedia from "./logos/expedia.svg";
import LogoSaxo from "./logos/SaxoBank.svg";
import LogoGrofers from "./logos/grofers.png";
import LogoTypeform from "./logos/typeform.svg";
import LogoSpothero from "./logos/SpotHero.png";
import LogoGeotab from "./logos/geotab.jpg";
import LogoThoughtworks from "./logos/Thoughtworks.png";
import LogoViasat from "./logos/viasat.png";
import LogoKlarna from "./logos/klarna.svg";
import LogoWolt from "./logos/wolt.png";
// logos for ingestion sources
import LogoAirflow from "./logos/airflow.svg";
import LogoAthena from "./logos/athena.png";
import LogoBigquery from "./logos/bigquery.svg";
import LogoDbt from "./logos/dbt.png";
import LogoDruid from "./logos/druid.svg";
import LogoGlue from "./logos/glue.png";
import LogoHive from "./logos/hive.svg";
import LogoKafka from "./logos/kafka.svg";
import LogoLdap from "./logos/ldap.png";
import LogoLooker from "./logos/looker.svg";
import LogoMongodb from "./logos/mongodb.svg";
import LogoMssql from "./logos/mssql.svg";
import LogoMysql from "./logos/mysql.svg";
import LogoOracle from "./logos/oracle.svg";
import LogoPostgres from "./logos/postgres.png";
import LogoRedshift from "./logos/redshift.svg";
import LogoSnowflake from "./logos/snowflake.svg";
import LogoSpark from "./logos/spark.svg";
import LogoSuperset from "./logos/superset.svg";
// images for articles carousel
import February2021Update from "./articles/february-2021-update.png";
import March2021Update from "./articles/march-2021-update.png";
import April2021Update from "./articles/april-2021-update.png";
import May2021Update from "./articles/may-2021-update.png";
import SaxoDatahub from "./articles/saxo-datahub.png";
import DataHubLineage from "./articles/datahub-lineage.png";

const features = [
  {
    title: "Open Source",
    imageUrl: "img/undraw_open_source_1qxw.svg",
    description: (
      <>
        DataHub was originally{" "}
        <Link to={"https://engineering.linkedin.com/blog/2019/data-hub"}>
          built at LinkedIn
        </Link>{" "}
        and subsequently{" "}
        <Link to={"https://github.com/linkedin/datahub"}>open-sourced</Link>{" "}
        under the Apache 2.0 License. It now has a thriving community with over
        a hundred contributors, and is widely used at many companies.
      </>
    ),
  },
  {
    title: "Forward Looking Architecture",
    imageUrl: "img/undraw_building_blocks_n0nc.svg",
    description: (
      <>
        DataHub follows a{" "}
        <Link
          to={
            "https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained"
          }
        >
          push-based architecture
        </Link>
        , which means it's built for continuously changing metadata. The modular
        design lets it scale with data growth at any organization, from a single
        database under your desk to multiple data centers spanning the globe.
      </>
    ),
  },
  {
    title: "Massive Ecosystem",
    imageUrl: "img/undraw_online_connection_6778.svg",
    description: (
      <>
        DataHub has pre-built integrations with your favorite systems: Kafka,
        Airflow, MySQL, SQL Server, Postgres, LDAP, Snowflake, Hive, BigQuery,
        and <Link to={"docs/metadata-ingestion"}>many others</Link>. The
        community is continuously adding more integrations, so this list keeps
        getting longer and longer.
      </>
    ),
  },
];

const svgFormatter = (Logo, className) => {
  return (
    <Logo
      className={clsx(styles.logo_image, className)}
      width="100%"
      height="100%"
    />
  );
};
const pngFormatter = (src, className) => {
  return (
    <Image
      className={clsx(styles.logo_image, className)}
      img={src}
      width="100%"
    />
  );
};

const companyLogos = [
  {
    name: "LinkedIn",
    image: svgFormatter(LogoLinkedin),
  },
  {
    name: "Expedia Group",
    image: svgFormatter(LogoExpedia),
  },
  {
    name: "Saxo Bank",
    image: svgFormatter(LogoSaxo, clsx(styles.logo_image_small)),
  },
  {
    name: "Grofers",
    image: pngFormatter(LogoGrofers),
  },
  {
    name: "Typeform",
    image: svgFormatter(LogoTypeform),
  },
  {
    name: "SpotHero",
    image: pngFormatter(LogoSpothero, clsx(styles.logo_image_small)),
  },
  {
    name: "Geotab",
    image: pngFormatter(LogoGeotab),
  },
  {
    name: "ThoughtWorks",
    image: pngFormatter(LogoThoughtworks, clsx(styles.logo_image_large)),
  },
  {
    name: "Viasat",
    image: pngFormatter(LogoViasat),
  },
  {
    name: "Klarna",
    image: svgFormatter(LogoKlarna),
  },
  {
    name: "Wolt",
    image: pngFormatter(LogoWolt, clsx(styles.logo_image_square)),
  },
];

const sourceLogos = [
  {
    name: "Airflow",
    image: svgFormatter(LogoAirflow),
  },
  {
    name: "Athena",
    image: pngFormatter(LogoAthena, clsx(styles.logo_image_square)),
  },
  {
    name: "BigQuery",
    image: svgFormatter(LogoBigquery),
  },
  { name: "DBT", image: pngFormatter(LogoDbt) },
  { name: "Druid", image: svgFormatter(LogoDruid) },
  {
    name: "Glue",
    image: pngFormatter(LogoGlue),
  },
  {
    name: "Hive",
    image: svgFormatter(LogoHive, clsx(styles.logo_image_square)),
  },
  { name: "Kafka", image: svgFormatter(LogoKafka) },
  { name: "Looker", image: svgFormatter(LogoLooker) },
  {
    name: "LDAP",
    image: pngFormatter(LogoLdap, clsx(styles.logo_image_square)),
  },
  { name: "MongoDB", image: svgFormatter(LogoMongodb) },
  {
    name: "MSSQL",
    image: svgFormatter(LogoMssql),
  },
  {
    name: "MySQL",
    image: svgFormatter(LogoMysql),
  },
  { name: "Oracle", image: svgFormatter(LogoOracle) },
  { name: "PostgreSQL", image: pngFormatter(LogoPostgres) },
  {
    name: "Redshift",
    image: svgFormatter(LogoRedshift),
  },
  { name: "Snowflake", image: svgFormatter(LogoSnowflake) },
  { name: "Spark", image: svgFormatter(LogoSpark) },
  {
    name: "Superset",
    image: svgFormatter(LogoSuperset),
  },
];

const videos = [
  { id: "VY57iRdG-Us" },
  { id: "fEILyoWVpBw" },
  { id: "3wiaqhb8UR0" },
  { id: "dlFa4ubJ9ho" },
  { id: "RQBEJhcen5E" },
  { id: "xE8Uc27VTG4" },
  { id: "mjKjjtm8GfM" },
  { id: "r862MZTLAJ0" },
];

const articles = [
  {
    name: "Enabling Data Discovery in a Data Mesh: The Saxo Journey",
    link: "https://medium.com/datahub-project/enabling-data-discovery-in-a-data-mesh-the-saxo-journey-451b06969c8f",
    image: pngFormatter(SaxoDatahub, clsx(styles.logo_image_huge)),
  },
  {
    name: "LinkedIn DataHub Project Updates (May 2021)",
    link: "https://medium.com/datahub-project/linkedin-datahub-project-updates-ed98cdf913c1",
    image: pngFormatter(May2021Update, clsx(styles.logo_image_huge)),
  },
  {
    name: "Data in Context: Lineage Explorer in DataHub",
    link: "https://medium.com/datahub-project/data-in-context-lineage-explorer-in-datahub-a53a9a476dc4",
    image: pngFormatter(DataHubLineage, clsx(styles.logo_image_huge)),
  },
  {
    name: "LinkedIn DataHub Project Updates (April 2021)",
    link: "https://medium.com/datahub-project/linkedin-datahub-project-updates-2b0d26066b8f",
    image: pngFormatter(April2021Update, clsx(styles.logo_image_huge)),
  },
  {
    name: "LinkedIn DataHub Project Updates (March 2021)",
    link: "https://medium.com/datahub-project/linkedin-datahub-project-updates-697f0faddd10",
    image: pngFormatter(March2021Update, clsx(styles.logo_image_huge)),
  },
  {
    name: "LinkedIn DataHub Project Updates (February 2021)",
    link: "https://medium.com/datahub-project/linkedin-datahub-project-updates-february-2021-edition-338d2c6021f0",
    image: pngFormatter(February2021Update, clsx(styles.logo_image_huge)),
  },
];

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

function Feature({ imageUrl, title, description }) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={clsx("col col--4", styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={styles.featureImage} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={clsx("hero", styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className="col col--6">
              <h1
                className={clsx(
                  "hero__title",
                  styles.not_bold_text,
                  styles.centerTextMobile
                )}
              >
                {siteConfig.tagline}
              </h1>
              <p className={clsx("hero__subtitle", styles.centerTextMobile)}>
                {/* DataHub helps you understand your data where it lives. */}
                {/* DataHub is a stream-first metadata platform that powers multiple vertical applications: powerful search &amp; discovery, data ops &amp; data quality, compliance, data management, and access control. */}
                Data ecosystems are diverse &#8212; too diverse. DataHub&#39;s
                extensible metadata platform enables data discovery, data
                observability and federated governance that helps you tame this
                complexity.
              </p>
              <div className={styles.buttons}>
                <Link
                  className={clsx(
                    "button button--primary button--lg",
                    styles.hero_button,
                    styles.hero_button_cta
                  )}
                  to={useBaseUrl("docs/")}
                >
                  Get Started
                </Link>
                <Link
                  className={clsx(
                    "button button--secondary button--outline button--lg",
                    styles.hero_button
                  )}
                  to="https://slack.datahubproject.io"
                >
                  Join our Slack
                </Link>
              </div>
            </div>
            <div className={clsx("col col--6", styles.bumpUpLogo)}>
              <div className={clsx("item shadow--tl")}>
                <img
                  src={useBaseUrl("img/screenshots/demo.gif")}
                  alt="Demo GIF"
                />
              </div>
            </div>
          </div>
        </div>
      </header>
      <section className={styles.big_padding_bottom}>
        {features && features.length > 0 && (
          <div className={styles.features}>
            <div className="container">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </div>
        )}
      </section>
      <section className={clsx(styles.section, styles.logo_section)}>
        <div className="container">
          <h1 className={clsx(styles.centerText, styles.small_padding_bottom)}>
            <span className={styles.larger_on_desktop}>
              Trusted Across the Industry
            </span>
          </h1>
          <div className={styles.logo_container}>
            {companyLogos.map((logo) => (
              <div key={logo.name}>
                <div className={styles.logo_frame}>
                  <div className={styles.logo_center}>{logo.image}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className={clsx(styles.section, styles.sources_section)}>
        <div className="container">
          <h1 className={clsx(styles.centerText, styles.small_padding_bottom)}>
            <span className={styles.larger_on_desktop}>
              Supported integrations
            </span>
          </h1>
          <div className={styles.logo_container}>
            {sourceLogos.map((logo) => (
              <div key={logo.name}>
                <div className={styles.logo_frame}>
                  <div className={styles.logo_center}>{logo.image}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className={clsx(styles.section)}>
        <div className="container">
          <h1 className={clsx(styles.centerText, styles.small_padding_bottom)}>
            <span className={styles.larger_on_desktop}>Learn more</span>
          </h1>
          <div className={styles.carousel_container}>
            <Slider
              dots={true}
              infinite={true}
              centerMode={true}
              slidesToShow={3}
              slidesToScroll={3}
              infinite={true}
              dots={true}
              autoplay={false}
              autoplaySpeed={3000}
              cssEase={"linear"}
              responsive={[
                {
                  breakpoint: 1080,
                  settings: {
                    slidesToShow: 2,
                    slidesToScroll: 2,
                  },
                },
                {
                  breakpoint: 960,
                  settings: {
                    slidesToShow: 2,
                    slidesToScroll: 1,
                  },
                },
                {
                  breakpoint: 720,
                  settings: {
                    slidesToShow: 1,
                    slidesToScroll: 1,
                  },
                },
              ]}
            >
              {articles.map((article) => {
                return (
                  <div
                    className={styles.carousel_logo_slide_large}
                    key={article.name}
                  >
                    <div className={styles.carousel_article_logo_frame}>
                      <div className={styles.carousel_article_logo_center}>
                        {article.image}
                      </div>
                      <div className={styles.carousel_article_title_center}>
                        <Link to={article.link} className={styles.article_link}>
                          {article.name}
                        </Link>
                      </div>
                    </div>
                  </div>
                );
              })}
            </Slider>
          </div>
          <div className={styles.sources_link}>
            <Link to={"https://medium.com/datahub-project"}>
              Read our Medium for more!
            </Link>
          </div>
        </div>
      </section>

      <section className={clsx(styles.section, styles.videos_section)}>
        <div className="container">
          <h1 className={clsx(styles.centerText, styles.small_padding_bottom)}>
            <span className={styles.larger_on_desktop}>Videos</span>
          </h1>
          <div className={styles.carousel_container}>
            <Slider
              dots={true}
              infinite={true}
              centerMode={true}
              slidesToShow={3}
              slidesToScroll={3}
              infinite={true}
              dots={true}
              autoplay={false}
              autoplaySpeed={3000}
              cssEase={"linear"}
              responsive={[
                {
                  breakpoint: 1080,
                  settings: {
                    slidesToShow: 2,
                    slidesToScroll: 1,
                  },
                },
                {
                  breakpoint: 720,
                  settings: {
                    slidesToShow: 1,
                    slidesToScroll: 1,
                  },
                },
              ]}
            >
              {videos.map((video) => {
                return (
                  <div className={styles.carousel_logo_slide} key={video.id}>
                    <YouTube
                      videoId={video.id}
                      opts={{
                        playerVars: {
                          // https://developers.google.com/youtube/player_parameters
                          autoplay: 0,
                        },
                      }}
                      containerClassName={styles.youtubeContainer}
                    />
                  </div>
                );
              })}
            </Slider>
          </div>
          <div className={styles.sources_link}>
            <Link
              to={
                "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w?sub_confirmation=1"
              }
            >
              Subscribe for more!
            </Link>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className="container">
          <h1 className={clsx(styles.centerText)}>
            <span className={styles.larger_on_desktop}>How does it work?</span>
          </h1>
          <div className={clsx("row", styles.section)}>
            <div className="col col--6">
              <h2>
                <span className={styles.larger_on_desktop}>
                  Automated Metadata Ingestion
                </span>
              </h2>
              {/* <p>
                There're two way to get metadata into DataHub: <b>push</b> and <b>pull</b>.
              </p> */}
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
                DataHub's push-based architecture also supports pull, but
                pull-first systems cannot support push. Learn more about
                metadata ingestion with DataHub in the{" "}
                <Link to={"docs/metadata-ingestion"}>docs</Link>.
              </p>
            </div>
            <div className="col col--6">
              <div className={styles.recipe_example}>
                <div className={styles.recipe_codeblock}>
                  <CodeBlock
                    className={"language-yml"}
                    metastring='title="recipe.yml"'
                  >
                    {example_recipe}
                  </CodeBlock>
                </div>
                <div className={styles.recipe_codeblock}>
                  <CodeBlock className={"language-shell"}>
                    {example_recipe_run}
                  </CodeBlock>
                </div>
              </div>
            </div>
          </div>

          <div className={clsx("row", styles.section)}>
            <div className="col col--6">
              <div className={clsx("item shadow--tl", styles.browse_image)}>
                <img
                  src="img/screenshots/browse.gif"
                  width="100%"
                  alt="Browsing DataHub"
                />
              </div>
            </div>
            <div className="col col--6">
              <h2 className={styles.big_margin_top}>
                <span className={styles.larger_on_desktop}>
                  Discover Trusted Data
                </span>
              </h2>
              <p className={styles.larger_on_desktop}>
                Browse and search over a continuously updated catalog of
                datasets, dashboards, charts, ML models, and more.
              </p>
            </div>
          </div>

          <div className={clsx("row", styles.section)}>
            <div className="col col--6">
              <h2 className={styles.massive_margin_top}>
                <span className={styles.larger_on_desktop}>
                  Understand Data in Context
                </span>
              </h2>
              <p className={styles.larger_on_desktop}>
                DataHub is the one-stop shop for documentation, schemas,
                ownership, lineage, pipelines and usage information. Data
                quality and data preview information coming soon.
              </p>
            </div>
            <div className="col col--6">
              <div className={clsx("item shadow--tl", styles.how_third_image)}>
                <img
                  src="img/screenshots/context.gif"
                  width="100%"
                  alt="DataHub Dataset View"
                />
              </div>
            </div>
          </div>
        </div>
      </section>
    </Layout>
  );
}

export default Home;
