import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quickstartcta.module.scss";
import CodeBlock from "@theme/CodeBlock";

const QuickstartCTA = () => {
  return (
    <div className={styles.quickstart__content}>
      <h1 className={styles.quickstart__title}>Get Started Now</h1>
      <p className={styles.quickstart__subtitle}>Run the following command to get started with DataHub.</p>
      <div className={styles.quickstart__codeblock}>
        <CodeBlock className={"language-shell"}>
          python3 -m pip install --upgrade pip wheel setuptools <br />
          python3 -m pip install --upgrade acryl-datahub <br />
          datahub docker quickstart
        </CodeBlock>
      </div>
      <div className={styles.quickstart__buttons}>
        <Link className="button button--primary button--md" to={useBaseUrl("docs/quickstart")}>
          Quickstart With Open Source
        </Link>
        <Link className="button button--secondary button--md" to="https://datahub.com/products/why-datahub-cloud/">
          Learn About DataHub Cloud
        </Link>
    </div>  
  </div>
  );
};

export default QuickstartCTA;
