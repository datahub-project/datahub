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
import clsx from "clsx";
import quickstartData from "./_content/observeQuickstartContent";
import heroContent from "./_content/observeHeroContent";
import caseStudyContent from "./_content/observeCaseStudyContent";
import IntegrationsStatic from "../_components/IntegrationsStatic";
import tilesContent from "./_content/observeTilesContent";
import testimonialsData from "./_content/observeTestimonialsContent";
import resourceData from "./_content/observeResourceContent";
import UnifiedTabs from "../_components/UnifiedTabs";
import unifiedTabsData from "./_content/observeUnifiedTabsContent";
import trialsContent from "./_content/observeTrialsContent";
import Trials from "../_components/Trials";
import SlidingTabs from "../_components/SlidingTabs";

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
      <IntegrationsStatic />
      <QuickstartContent quickstartContent={quickstartData} />
      <Testimonials testimonialsData={testimonialsData} />
      <UnifiedTabs unifiedTabsData={unifiedTabsData}/>
      <div className={clsx("testimonials", styles.testimonials)}>
      <div className="testimonials__content">
        <div className="testimonials__card">
          <div className="testimonials__meta">
            <div className="testimonials__logo">
              <img src={useBaseUrl("/img/solutions/logo-depop.png")} />
            </div>
            <div className="testimonials__company">
              <div className="testimonials__company_title">Depop</div>
              <div className="testimonials__author_title">Olivier Tatard <br />Engineering Manager</div>
            </div>
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
      <SlidingTabs />
      <Trials onOpenTourModal={onOpenTourModal} trialsContent={trialsContent} />
      <div className={styles.resource_container}>
      <div className={styles.resource}>
        <div className={styles.resource_heading}>
          Resources
        </div>
        <div className={styles.card_row}>
          <div className={styles.card_row_wrapper}>
            {resourceData.map((resource) => (
              <div className={styles.card} key={resource.link}>
                <a className={styles.cardLink} href={resource.link}>
                  {resource.tag ? <span className={styles.card_tag}>{resource.tag}</span> : null}
                  <div class={styles.read_time}>{resource.readTime} mins read</div>
                  <div className={styles.card_image} style={{ backgroundImage: `url(${resource.backgroundImage})` }}>
                    <div className={styles.cardImageBackground} />
                  </div>
                  <div className={styles.card_content}>
                    <div className={styles.card_heading}>
                      {resource.title}
                    </div>
                    <div className={styles.read_more}>
                      <a href={resource.link} className={styles.read_more}>
                        Read more
                      </a>
                    </div>
                  </div>
                </a>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
    </Layout>
  ) : null;
}

export default Home;