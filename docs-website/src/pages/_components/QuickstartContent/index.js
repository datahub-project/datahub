import React from "react";
import clsx from "clsx";
// import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
// import Image from "@theme/IdealImage";
// import { useColorMode } from "@docusaurus/theme-common";
// import { QuestionCircleOutlined } from "@ant-design/icons";
import styles from "./quickstartcontent.module.scss";
// import CodeBlock from "@theme/CodeBlock";
// import TownhallButton from "../TownhallButton";
// import { Section } from "../Section";

const QuickstartContent = ({}) => {
  // const { colorMode } = useColorMode();
  return (
    <div className={clsx("quickstart", styles.quickstart)}>
      <div className="quickstart__header">
        <div className="quickstart__title">The only platform you need</div>
        <div className="quickstart__subtitle">
          Unified Discovery, Observability, and Governance for Data and AI.
        </div>
      </div>
      <div className="quickstart__content">
        <div className="quickstart__text">
          <div className="quickstart__text__label">Governance</div>
          <div className="quickstart__text__head">
            Minimize compliance risk, effortlessly
          </div>
          <div className="quickstart__text__desc">
            <p>
              Ensure every data asset is accounted for and responsibility
              governed by defining and enforcing documentation standards.
            </p>

            <p>
              Automate your governance program to automatically classify assets
              as they evolve over time.
            </p>

            <p>
              {" "}
              Minimize redundant, manual work with GenAI documentation,
              AI-driven classification, smart propagation, and more.
            </p>
            <span className="learn_more">Learn More →</span>
          </div>
        </div>
        <div className="quickstart__img">
          <img src={useBaseUrl("/img/quickstart.png")} />
        </div>
      </div>
    </div>
  );
};

export default QuickstartContent;
