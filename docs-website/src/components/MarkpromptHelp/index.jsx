import React, { useContext, useEffect, useState } from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { usePluginData } from "@docusaurus/useGlobalData";
import "@markprompt/css"
import { Markprompt } from "@markprompt/react";
import styles from "./markprompthelp.module.scss";
import { VisuallyHidden } from "@radix-ui/react-visually-hidden";

import { LikeOutlined, DislikeOutlined } from "@ant-design/icons";

const MarkpromptHelp = () => {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Markprompt projectKey={siteConfig.customFields.markpromptProjectKey} model="gpt-4">
    </Markprompt>
  );
};



export default MarkpromptHelp;
