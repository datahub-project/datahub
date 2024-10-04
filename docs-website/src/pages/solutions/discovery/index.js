import React, { useState } from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Hero from "../../_components/Hero";
import QuickstartContent from "../../_components/QuickstartContent";
import Ecosystem from "../../_components/Ecosystem";
import Community from "../../_components/Community";
import SocialMedia from "../../_components/SocialMedia";
import CaseStudy from "../../_components/CaseStudy";
import styles from "./styles.module.scss";
import CloseButton from "@ant-design/icons/CloseCircleFilled";
import Link from "@docusaurus/Link";
import clsx from "clsx";

const companyIndexes = require("../../../../adoptionStoriesIndexes.json");
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
      </div>
      <QuickstartContent />
      <div className={clsx("testimonials", styles.testimonials)}>
        <div className="testimonials__content">
          <div className="testimonials__card">
            <div className="testimonials__text">
              <div className="testimonials__quote_title">
                Enter end-to-end <br />Data Discovery.
              </div>
              <div className="testimonials__quote_description">
                Seamlessly integrated with DataHub Cloud's <br /> Data Observability & Governance solutions.
              </div>
            </div>
            <div className="testimonials__logo">
              <img src={useBaseUrl("/img/solutions/discovery-icons-group.png")} />
            </div>
          </div>
        </div>
      </div>
      <Ecosystem />
      <SocialMedia />
      <CaseStudy />
      <div className={styles.container}>
      <div className={styles.trial}>
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
      </div>
    </div>
    </Layout>
  ) : null;
}

export default Home;
