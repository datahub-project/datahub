import React, { useState } from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Hero from "../_components/Hero";
import QuickstartContent from "../_components/QuickstartContent";
import Tiles from "../_components/Tiles";
import Trials from "../_components/Trials";
import Testimonials from "../_components/Testimonials";
import CaseStudy from "../_components/CaseStudy";
import CloseButton from "@ant-design/icons/CloseCircleFilled";
import quickstartData from "./_content/discoveryQuickstartContent";
import heroContent from "./_content/discoveryHeroContent";
import caseStudyContent from "./_content/discoveryCaseStudyContent";
import Integrations from "../_components/Integrations";
import tilesContent from "./_content/discoveryTilesContent";
import testimonialsData from "./_content/discoveryTestimonialsContent";
import trialsContent from "./_content/discoveryTrialsContent";

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
