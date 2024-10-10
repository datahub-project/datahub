import React, { useState } from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Hero from "../_components/Hero";
import Tiles from "../_components/Tiles";
import Testimonials from "../_components/Testimonials";
import CaseStudy from "../_components/CaseStudy";
import QuickstartContent from "../_components/QuickstartContent";
import styles from "./styles.module.scss";
import CloseButton from "@ant-design/icons/CloseCircleFilled";
import Link from "@docusaurus/Link";
import clsx from "clsx";
import quickstartData from "./_content/observeQuickstartContent";
import heroContent from "./_content/observeHeroContent";
import caseStudyContent from "./_content/observeCaseStudyContent";
import Integrations from "../_components/Integrations";
import tilesContent from "./_content/observeTilesContent";
import testimonialsData from "./_content/observeTestimonialsContent";

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
      description="DataHub is a data discovery application built on an extensible data catalog that helps you tame the complexity of diverse data ecosystems."
    >
      {isTourModalVisible ? (
        <div className="tourModal">
          <div className="closeButtonWrapper" onClick={onCloseTourModal}>
            <CloseButton />
          </div>
          <iframe src="https://www.acryldata.io/tour" />
        </div>
      ) : null}
      <Hero onOpenTourModal={onOpenTourModal} heroContent={heroContent}/>
      <Integrations />
      <QuickstartContent quickstartContent={quickstartData} />
      <Testimonials testimonialsData={testimonialsData} />
      <div className={clsx("testimonials", styles.testimonials)}>
      <div className="testimonials__content">
        <div className="testimonials__card">
          <div className="testimonials__logo">
            <img src={useBaseUrl("/img/solutions/logo-depop.png")} />
          </div>
          <div className="testimonials__company">
            <div className="testimonials__company_title">Depop</div>
            <div className="testimonials__author_title">Olivier Tatard <br />Engineering Manager</div>
          </div>
          <div className="testimonials__text">
            "We chose Acryl because we see the value of having both a data catalog and observability capabilities in one tool. Having data owners, maintainers, and consumers in one place streamlines incident management and allows for faster time to resolution."
          </div>
        </div>
      </div>
    </div>
      <div>
        {tilesContent.map((content, index) => (
          <Tiles key={index} tilesContent={content} />
        ))}
      </div>
      <CaseStudy caseStudyContent= {caseStudyContent}/>
      <div className={styles.container}>
        <div className={styles.trial}>
        <div className={styles.trial_right}>
          <div className={styles.right_content}>
            <div className={styles.gradientTop} />
            <div className={styles.gradientBottom} />
            <div className={styles.right_l}>
              <div className={styles.soc}>
                <img
                  width={60}
                  height={60}
                  src={useBaseUrl("/img/lock-soc.svg")}
                />
                Protect your<br/>mission-critical <br/>tables, reports,<br/>services, and more.
              </div>
              <div className={styles.cost}>
                <img
                  width={60}
                  height={60}
                  src={useBaseUrl("/img/dollar.svg")}
                />
                Let AI detect the <br />blindspots in your <br/>data quality checks.
              </div>
            </div>
            <div className={styles.right_r}>
              <div className={styles.enterprise}>
                <img
                  width={60}
                  height={60}
                  src={useBaseUrl("/img/building.svg")}
                />
                Know first, not last.<br/>Get notified where <br/>you work when <br/>things go wrong.
              </div>
              <div className={styles.link}>
                <img width={60} height={75} src={useBaseUrl("/img/link.svg")} />
                Share documentation, <br/>compliance and health <br/>for any data asset with <br/> one link.
              </div>
            </div>
          </div>
        </div>
          <div className={styles.trial_left}>
            <div className={styles.left_content}>
              <p className={styles.trial_title}>
                Start building trust<br/>with your stakeholders, <br/> today.
              </p>
              <div className={styles.btn_div}>
                <Link to="/cloud">Book a Demo</Link>
                <a
                  onClick={onOpenTourModal}
                >Product Tour</a>
              </div>
              <Link className={styles.start_arrow} to="/docs">Get started with Core →</Link>
            </div>
          </div>
        </div>
      </div>
    </Layout>
  ) : null;
}

export default Home;