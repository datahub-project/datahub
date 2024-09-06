import React from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Enterprise from "./Enterprise";
import { Section } from "../_components/Section";
import ScrollingCustomers from "./CompanyLogos";
import clsx from "clsx";
import styles from "./styles.module.scss";
import UnifiedTabs from "./UnifiedTabs";
import FeatureCards from "./FeatureCards";


function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;
  // const { colorMode } = useColorMode();


  if (siteConfig.customFields.isSaas) {
    window.location.replace("/docs");
  }

  return !siteConfig.customFields.isSaas ? (
    <Layout
      title={'DataHub Cloud - Unify Data Observability, Governance and Discovery'}
      description="DataHub cloud is Managed DataHub with Data Observability and Data Governance built-in."
    >
      <header className={clsx("hero", styles.hero)}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className={clsx("hero__title", styles.hero__title)}>Try DataHub Cloud</h1>
              <div className={clsx("hero__subtitle", styles.hero__subtitle)}>
                Introducing DataHub as a Managed Service
                <div style={{ fontWeight: "500"}}>with Data Observability and Data Governance built-in.</div>
                {/* <div className={styles.learnMore}>
                  <a className={styles.link} href="https://acryldata.io" target="_blank">
                    Learn More  →
                  </a>
                </div> */}
              </div>
              <Link className="button button--primary button--lg" to="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup">
                Book Demo
              </Link>
              <Link className={clsx(styles.buttonLightBlue, "button  button--secondary button--lg")} to="https://www.acryldata.io/tour">
                Product Tour
              </Link>
            </div>
          </div>
        </div>
      </header>
      <ScrollingCustomers />
      <div className={clsx(styles.bgSection)} style={{ marginTop: 100 }}>
        <UnifiedTabs />
      </div>
      <FeatureCards/>
      <div className={clsx(styles.bgSection)}>
        <Section>
          <Enterprise />
        </Section>
      </div>
      <div className={clsx("hero", styles.hero)}>
        <div className="container" style={{ paddingTop: '12vh', paddingBottom: '12vh' }}>
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Get your free trial.</h1>
              <div className={clsx(styles.hero__secondtitle)}>Data Discovery, Data Quality and Data Governance unified.</div>

              <Link className="button button--primary button--lg" to="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup">
                Book Demo
              </Link>
              <Link className={clsx(styles.buttonLightBlue, "button button--secondary button--lg")} to="https://www.acryldata.io/tour">
                Product Tour
              </Link>
              <div className="hero__subtitle" />
              {/* <hr style={{margin: "3rem"}}/>
              <div className="hero__subtitle">
                An extension of the DataHub Core project.<br/>
                <a className={clsx(styles.link)} href="/docs/managed-datahub/managed-datahub-overview">View Cloud Docs.
                </a>
              </div> */}
            </div>
          </div>
        </div>
      </div>
    </Layout>
  ) : null;
}

export default Home;
