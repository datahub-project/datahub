import React from "react";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { FilterPage } from "./docs/_components/FilterPage";
import { FastBackwardFilled } from "@ant-design/icons";
const integrations = require("../../../docs/generated/ingestion/integrations.json");
const metadata = integrations.ingestionSources;

function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return FilterPage(
    siteConfig,
    metadata,
    "DataHub Integrations",
    `Connect to ${Math.floor(metadata.length / 10) * 10}+ data and AI systems`,
    false,
    true,
    false,
    "DataHub Connector Directory: 140+ Integrations",
    "Find every system DataHub integrates with. Browse 140+ DataHub connectors across databases, BI tools, ETL, AI/ML systems, and more."
  );
}

export default DataProviderComponent;
