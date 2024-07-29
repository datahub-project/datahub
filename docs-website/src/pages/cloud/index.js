import React from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Features from "./Enterprise";
import { Section, PromoSection } from "../_components/Section";
import { useColorMode } from "@docusaurus/theme-common";
import { CompanyLogos } from "./CompanyLogos";
import clsx from "clsx";
import styles from "./styles.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { QuestionCircleOutlined } from "@ant-design/icons";
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
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible data catalog that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={clsx("hero", styles.hero)}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Try DataHub Cloud</h1>
              <div className="hero__subtitle">
                DataHub as a Managed Service with additional capabilities.
                <div className={styles.learnMore}>
                  <a href="/">
                    Learn More  →
                  </a>
                </div>
              </div>
              <Link className="button button--primary button--md" to="/">
                Book Demo
              </Link>
              <Link className={clsx(styles.buttonLightBlue, "button  button--secondary button--md")} to="/">
                Product Tour
              </Link>
            </div>
          </div>
        </div>
      </header>
      <CompanyLogos />
      <div className={clsx(styles.bgSection)}>
        <UnifiedTabs />
      </div>
      <Section>
        <FeatureCards/>
      </Section>
      <div className={clsx(styles.bgSection)}>
        <Section>
          <Features />
        </Section>
      </div>
      <div className={clsx("hero", styles.hero)}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Get Started With <br /> DataHub Cloud Today.</h1>

              <Link className="button button--primary button--md" to="/">
                Book Demo
              </Link>
              <Link className={clsx(styles.buttonLightBlue, "button button--secondary button--md")} to="/">
                Product Tour
              </Link>
              <hr style={{margin: "3rem"}}/>
              <div className="hero__subtitle">
              Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam commodo ultricies semper. <a href="/">View Cloud Docs.</a>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Layout>
  ) : null;
}

export default Home;
