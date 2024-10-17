import React, { useState } from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Hero from "../_components/Hero";
import QuickstartContent from "../_components/QuickstartContent";
import Tiles from "../_components/Tiles";
import Testimonials from "../_components/Testimonials";
import CaseStudy from "../_components/CaseStudy";
import Persona from "../_components/Persona";
import styles from "./styles.module.scss";
import CloseButton from "@ant-design/icons/CloseCircleFilled";
import Link from "@docusaurus/Link";
import quickstartData from "./_content/governanceQuickstartContent";
import heroContent from "./_content/governanceHeroContent";
import caseStudyContent from "./_content/governanceCaseStudyContent";
import personaContent from "./_content/governancePersonaContent";
import tilesContent from "./_content/governanceTilesContent";
import testimonialsData from "./_content/governanceTestimonialsContent";
import trialsContent from "./_content/governanceTrialsContent";
import Trials from "../_components/Trials";

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
      <Persona personaContent={personaContent} />
      <QuickstartContent quickstartContent={quickstartData} />
      <Testimonials testimonialsData={testimonialsData} />
      <div>
        {tilesContent.map((content, index) => (
          <Tiles key={index} tilesContent={content} />
        ))}
      </div>
      <CaseStudy caseStudyContent= {caseStudyContent}/>
      <Trials onOpenTourModal={onOpenTourModal} trialsContent={trialsContent} />
    </Layout>
  ) : null;
}

export default Home;