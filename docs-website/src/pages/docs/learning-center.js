import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import SearchBar from "./_components/SearchBar";
import LearningCenterCards from "./_components/LearningCenterCards";
import GuideList from "./_components/GuideList";
import FeatureAvailability from '@site/src/components/FeatureAvailability';


import {
  FolderTwoTone,
  BookTwoTone,
  TagsTwoTone,
  ApiTwoTone,
  SearchOutlined,
  CompassTwoTone,
  NodeExpandOutlined,
  CheckCircleTwoTone,
  SafetyCertificateTwoTone,
  LockTwoTone,
  SlackOutlined,
  HistoryOutlined,
} from "@ant-design/icons";


function Docs() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Learning Center</h1>
              <p className="hero__subtitle">Guides and tutorials for everything DataHub.</p>
              <SearchBar />
            </div>
          </div>
          <LearningCenterCards />
        </div>
      </header>
    </Layout>
  );
}

export default Docs;
