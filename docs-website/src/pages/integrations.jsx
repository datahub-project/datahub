import React from "react";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { FilterPage } from "./docs/_components/FilterPage";
import { FastBackwardFilled } from "@ant-design/icons";
const integrations = require("../../../docs/generated/ingestion/integrations.json");
const metadata = integrations.ingestionSources;

function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;
  const connectorCount = Math.floor(metadata.length / 10) * 10;

  return FilterPage(
    siteConfig,
    metadata,
    "DataHub Integrations",
    `Connect to ${connectorCount}+ data and AI systems`,
    false, // allowExclusivity
    true, // useTags
    false, // useFilters
    `DataHub Connector Directory: ${connectorCount}+ Integrations`, // seoTitle
    `Find every system DataHub integrates with. Browse ${connectorCount}+ DataHub connectors across databases, BI tools, ETL, AI/ML systems, and more.`, // seoDescription
    true // showLegend
  );
}

export default DataProviderComponent;
