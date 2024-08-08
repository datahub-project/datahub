import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Image from "@theme/IdealImage";
import { useColorMode } from "@docusaurus/theme-common";
import { QuestionCircleOutlined } from "@ant-design/icons";
import styles from "./quickstartcontent.module.scss";
import CodeBlock from "@theme/CodeBlock";
import TownhallButton from "../TownhallButton";
import { Section } from "../Section";


const QuickstartContent = ({}) => {
  const { colorMode } = useColorMode();
  return (
      <div className={clsx("container", styles.container)}>
        <div className="hero__content">
           <div className={styles.quickstartContent}>
              <h1 className={styles.quickstartTitle}>Get Started Now</h1>
              <p className={styles.quickstartSubtitle}>Run the following command to get started with DataHub.</p>
              <div className={styles.quickstartCodeblock}>
                <CodeBlock className={"language-shell"}>
                  python3 -m pip install --upgrade pip wheel setuptools <br />
                  python3 -m pip install --upgrade acryl-datahub <br />
                  datahub docker quickstart
                </CodeBlock>
              </div>
              <Link className={clsx("button button--primary button--md", styles.button)} to={useBaseUrl("docs/quickstart")}>
                DataHub Quickstart Guide
              </Link>
              <Link className={clsx("button button--secondary button--md", styles.button)} to={useBaseUrl("docs/quickstart")}>
                Deploying With Kubernetes
              </Link>
          </div>
          <div className={clsx("card", styles.quickLinks)}>
          <div className={styles.quickLinksLabel}>
            <QuestionCircleOutlined />
            Learn
          </div>
          <Link to={useBaseUrl("docs/")}>What is DataHub?</Link>
          <Link to={useBaseUrl("docs/architecture/architecture")}>How is DataHub architected?</Link>
          <Link to="https://demo.datahubproject.io">See DataHub in action</Link>
          </div>
        </div>
      </div>
  );
};

export default QuickstartContent;
