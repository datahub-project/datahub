import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import FilterBar from "./_components/FilterBar";
import FeatureAvailability from '@site/src/components/FeatureAvailability';
import FilterCards from "./_components/FilterCards";
import useGlobalData from '@docusaurus/useGlobalData';


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
  const globalData = useGlobalData();
  const myPluginData = globalData['docusaurus-plugin-content-docs']['default'].versions[0].docs;
  var sourcesList = myPluginData.map(doc => {
    if (doc.path.includes("/docs/generated/ingestion/sources/")){
      doc.path=doc.path.replace("/docs/generated/ingestion/sources/", "");
      doc.path=doc.path.replace(".md", "");
      return doc.path
    }
  });
  sourcesList = sourcesList.filter(function( element ) {
    return element !== undefined;
 });
  const ingestionSourceContent = sourcesList.map(source =>{
  return {
    title: source,
    platformIcon: source,
    description: source,
    to: "docs/generated/ingestion/sources/" + source,
  };
} )
console.log(ingestionSourceContent)

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">DataHub Integrations</h1>
              <p className="hero__subtitle">Services that integrate with DataHub</p>
              <FilterBar />
            </div>
          </div>
          <FilterCards  content ={ingestionSourceContent} filterBar= {<FilterBar />} />
        </div>
      </header>
    </Layout>
  );
}

export default Docs;
