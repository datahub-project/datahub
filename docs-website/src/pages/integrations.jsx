import React from "react";

import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { FilterPage } from "./docs/_components/FilterPage";
import { FastBackwardFilled } from "@ant-design/icons";
const filterTagIndexes = require("../../filterTagIndexes.json");
const metadata = filterTagIndexes.ingestionSources;

function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return FilterPage(
    siteConfig,
    metadata,
    "DataHub Integrations",
<<<<<<< HEAD
    "Services that integrate with DataHub",
    false,
    true
=======
    "Services that integrate with DataHub"
>>>>>>> c4d1d82a0 (docs(): add sources summary page (#7480))
  );
}

export default DataProviderComponent;
