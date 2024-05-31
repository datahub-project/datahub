import React from "react";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { UseCasePage } from "./docs/_components/UseCasePage";
import { FastBackwardFilled } from "@ant-design/icons";
const useCaseIndexes = require("../../useCaseIndexes.json");
const metadata = useCaseIndexes.useCases;

function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return UseCasePage(
    siteConfig,
    metadata,
    "DataHub Learn",
    "Learn about the hot topics in the data ecosystem and how DataHub can help you with your data journey.",
    false,
    true
  );
}

export default DataProviderComponent;
