import React from "react";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { FilterPage } from "./docs/_components/FilterPage";
const filterTagIndexes = require("../../filterTagIndexes.json");
const metadata = filterTagIndexes.ingestionSources;

function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return FilterPage(
    siteConfig,
    metadata,
    "DataHub Integrations",
    "Services that integrate with DataHub",
    true,
    true
  );
}

export default DataProviderComponent;
