import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import ThemedImage from '@theme/ThemedImage';
import styles from './styles.module.css';

const features = [
  {
    title: 'Open Source',
    imageUrl: 'img/undraw_open_source_1qxw.svg',
    description: (
      <>
        DataHub was originally <Link to={"https://engineering.linkedin.com/blog/2019/data-hub"}>built
        at LinkedIn</Link> and subsequently <Link to={"https://github.com/linkedin/datahub"}>open-sourced</Link> under
        the Apache 2.0 License. It now has a thriving community with over 75 contributors.
      </>
    ),
  },
  {
    title: 'Forward Looking Architecture',
    imageUrl: 'img/undraw_building_blocks_n0nc.svg',
    description: (
      <>
        DataHub follows a <Link to={"https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained"}>push-based architecture</Link>,
        which lets it support advanced use cases and scale with an organization while not being too complex when getting started.
      </>
    ),
  },
  {
    title: 'Massive Ecosystem',
    imageUrl: 'img/undraw_online_connection_6778.svg',
    description: (
      // TODO: update the integrations link to scroll down the page.
      <>
        DataHub has pre-built integrations with Kafka, MySQL, MS SQL, Postgres, LDAP, Snowflake,
        Hive, BigQuery, and <Link to={"docs/metadata-ingestion"}>many others</Link>.
      </>
    ),
  },
];

function Feature({ imageUrl, title, description }) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={clsx('col col--4', styles.feature)}>
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
      title={`${siteConfig.title} - ${siteConfig.tagline}`}
      description="Description will go into a meta tag in <head />">
      <header className={clsx('hero', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className="col col--8">
              <h1 className={clsx("hero__title", styles.not_bold_text, styles.centerTextMobile)}>{siteConfig.tagline}</h1>
              <p className={clsx("hero__subtitle", styles.centerTextMobile)}>TODO: a brief description of what datahub is and why it might be interesting to a company. Can be a few lines</p>
              <div className={styles.buttons}>
                <Link
                  className={clsx(
                    'button button--primary button--lg', styles.hero_button
                  )}
                  to={useBaseUrl('docs/')}>
                  Get Started
            </Link>
                <Link
                  className={clsx(
                    'button button--secondary button--outline button--lg',
                    styles.hero_button
                  )}
                  to='https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA'>
                  Join our Slack
            </Link>
              </div>
            </div>
            <div className={clsx("col col--4", styles.hiddenMobile, styles.bumpUpLogo)}>
              <ThemedImage
                alt="DataHub Logo"
                sources={{
                  light: useBaseUrl(siteConfig.themeConfig.navbar.logo.src),
                  dark: useBaseUrl(siteConfig.themeConfig.navbar.logo.srcDark),
                }}
              />
            </div>
          </div>
        </div>
      </header>
      <section>
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
      <section className="hero hero--dark">
        <div className="container">
          TODO
        </div>
      </section>

    </Layout>
  );
}

export default Home;
