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
    "Services that integrate with DataHub",
    false,
    true
  );
}

export default DataProviderComponent;
