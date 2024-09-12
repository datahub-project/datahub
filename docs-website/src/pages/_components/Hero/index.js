import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
// import Image from "@theme/IdealImage";
// import { useColorMode } from "@docusaurus/theme-common";
// import { QuestionCircleOutlined } from "@ant-design/icons";
import styles from "./hero.module.scss";
// import CodeBlock from "@theme/CodeBlock";
// import TownhallButton from "../TownhallButton";
// import { Section } from "../Section";

// const HeroAnnouncement = ({ message, linkUrl, linkText }) => (
//   <div className={clsx("hero__alert alert alert--primary", styles.hero__alert)}>
//     <span>{message}</span>
//     {linkUrl && (
//       <Link className="button button--primary button--md" href={linkUrl} target="_blank">
//         {linkText}
//       </Link>
//     )}
//   </div>
// );

const Hero = ({}) => {
  // const { colorMode } = useColorMode();
  return (
    <header className={clsx("hero", styles.hero)}>
      <div className="container">
        <div className="hero__content">
          <div className="hero__text">
            <div className="hero__title">
              The <strong>#1 open source</strong> metadata platform.
            </div>
            <div className="hero__subtitle">
              A unified platform for{" "}
              <span>
                {" "}
                <span>AI Governance</span>
              </span>
            </div>
            <div className="hero__cta">
              <Link className="cta__primary" to="/cloud">Book a Demo</Link>
              <Link className="cta__secondary" to="https://www.acryldata.io/tour">Product Tour</Link>
            </div>
            <Link className="hero__footer_cta" to="/docs">Get started with Core →</Link>
          </div>
          <div className="hero__img">
            <img src={useBaseUrl("/img/hero.png")} />
          </div>
        </div>
      </div>
    </header>
  );
};

export default Hero;
