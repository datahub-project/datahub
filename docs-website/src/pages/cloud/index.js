import React from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Enterprise from "./Enterprise";
import { Section } from "../_components/Section";
import clsx from "clsx";
import styles from "./styles.module.scss";
import UnifiedTabs from "./UnifiedTabs";
import FeatureCards from "./FeatureCards";
import Hero from "./Hero";
import DemoForm from "./DemoForm";

function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  if (siteConfig.customFields.isSaas) {
    window.location.replace("/docs");
  }

  return !siteConfig.customFields.isSaas ? (
    <Layout
      title={'DataHub Cloud - Unify Data Observability, Governance and Discovery'}
      description="DataHub cloud is Managed DataHub with Data Observability and Data Governance built-in."
    >
      <Hero />
      <div className={clsx(styles.bgSection)}>
        <UnifiedTabs />
      </div>
      <FeatureCards/>
      <div className={clsx(styles.bgSection)}>
        <Section>
          <Enterprise />
        </Section>
      </div>
      <div className={clsx(styles.weeklyDemoSection)}>
        <div>Curious? Drop by and say hi!</div>
        <Link to="https://www.acryldata.io/webinars/weekly-live-demo">Weekly Live Demos →</Link>
      </div>
      <div className={clsx("hero", styles.hero)}>
        <div className="container" style={{ paddingTop: '12vh', paddingBottom: '12vh' }}>
          <div className="row row__padded">
            <div  className={clsx(styles.col, styles.hero__cta, "col col--7")}>
              <h1 className={styles.hero__title}>Start your free trial<br/>today.</h1>
              <div className={clsx(styles.hero__subtitle)}>Unify Discovery, Observability and Governance<br/>for data and AI.</div>
              <div>
                <Link className={clsx(styles.button, styles.bookButton, "button button--primary button--lg")} to="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup">
                  Book Demo
                </Link>
                <Link className={clsx(styles.button, styles.productTourButton, "button button--secondary button--lg")} to="https://www.acryldata.io/tour">
                  Live Product Tour →
                </Link>
              </div>
              <div className="hero__subtitle" />
            </div>
            <div className={clsx(styles.col, "col col--5")}>
               <DemoForm formId="footerForm"/>
            </div>
          </div>
        </div>
      </div>
    </Layout>
  ) : null;
}

export default Home;
