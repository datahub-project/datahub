import React, { useEffect, useState } from "react";
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
import ServiceBell from "@servicebell/widget";
import DemoFormModal from "./DemoFormModal";

const SERVICE_BELL_ID = "00892146e5bc46d98d55ecc2b2fa67e2";

function Home() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  const [isModalOpen, setIsModalOpen] = useState(false);

  const handleOpenModal = () => setIsModalOpen(true);
  const handleCloseModal = () => setIsModalOpen(false);
  if (siteConfig.customFields.isSaas) {
    window.location.replace("/docs");
  }

  useEffect(() => {
    ServiceBell("init", SERVICE_BELL_ID, { hidden: false });
  }, []);

  return !siteConfig.customFields.isSaas ? (
    <Layout
      title={'DataHub Cloud - Unify Data Observability, Governance and Discovery'}
      description="DataHub cloud is Managed DataHub with Data Observability and Data Governance built-in."
    >
      <Hero />
      <div className={clsx(styles.bgSection)}>
        <UnifiedTabs />
      </div>
      <FeatureCards />
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
            <div className={clsx(styles.col, styles.hero__cta, "col col--7")}>
              <h1 className={styles.hero__title}>Start with DataHub Cloud<br />today.</h1>
              <div className={clsx(styles.hero__subtitle)}>
                Unify Discovery, Observability and Governance<br />for data and AI.
              </div>
              <div>
                <button
                  className={clsx(styles.button, styles.bookButton, "button button--primary button--lg")}
                  onClick={handleOpenModal}
                >
                  Book Demo
                </button>
                <Link className={clsx(styles.button, styles.productTourButton, "button button--secondary button--lg")} to="https://www.acryldata.io/tour">
                  Live Product Tour →
                </Link>
              </div>
              <div className="hero__subtitle" />
            </div>
            <div className={clsx(styles.col, "col col--5", styles.hideOnMobile)}>
              <DemoForm formId="footerForm" />
            </div>
          </div>
        </div>
      </div>
      {isModalOpen && (
        <DemoFormModal formId="footerFormMobile" handleCloseModal={handleCloseModal} />
      )}
    </Layout>
  ) : null;
}

export default Home;
