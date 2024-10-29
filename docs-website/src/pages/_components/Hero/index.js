import React, { useEffect } from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
// import Image from "@theme/IdealImage";
// import { useColorMode } from "@docusaurus/theme-common";
import { HeartOutlined } from "@ant-design/icons";
import styles from "./hero.module.scss";
<<<<<<< HEAD
import CodeBlock from "@theme/CodeBlock";
=======
import { animate, motion, useMotionValue, useTransform } from "framer-motion";
// import CodeBlock from "@theme/CodeBlock";
// import TownhallButton from "../TownhallButton";
// import { Section } from "../Section";
>>>>>>> upstream/master

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

const SOLUTION_TEXTS = ["AI Governance", "Data Discovery", "AI Collaboration", "Data Governance", "Data Democratization", "Data Observability"];

const Hero = ({ onOpenTourModal }) => {
  // const { colorMode } = useColorMode();
  const textIndex = useMotionValue(0);
  const baseText = useTransform(textIndex, (latest) => SOLUTION_TEXTS[latest] || "");
  const count = useMotionValue(0);
  const rounded = useTransform(count, (latest) => Math.round(latest));
  const displayText = useTransform(rounded, (latest) =>
    baseText.get().slice(0, latest)
  );
  const updatedThisRound = useMotionValue(true);

  useEffect(() => {
    animate(count, 60, {
      type: "tween",
      duration: 1.5,
      ease: "easeIn",
      repeat: Infinity,
      repeatType: "reverse",
      repeatDelay: 0.1,
      onUpdate(latest) {
        if (updatedThisRound.get() === true && latest > 0) {
          updatedThisRound.set(false);
        } else if (updatedThisRound.get() === false && latest === 0) {
          textIndex.set((textIndex.get() + 1) % SOLUTION_TEXTS.length);
          updatedThisRound.set(true);
        }
      },
    });
  }, []);
  return (
    <header className={clsx("hero", styles.hero)}>
      <div className="container">
        <div className="hero__content">
          <div className="hero__text">
            <div className="hero__title">
              The <strong>#1 open source</strong><br/>metadata platform.
            </div>
            <div className="hero__subtitle">
              A unified platform for
              <span>
                <motion.span>{displayText}</motion.span>
              </span>
            </div>
            <div className="hero__cta">
              <Link className="cta__primary" to="/cloud">
                Book a Demo
              </Link>
              <a
                className="cta__secondary"
                // to="https://www.acryldata.io/tour"
                onClick={onOpenTourModal}
              >
                Product Tour
              </a>
            </div>
            <Link className="hero__footer_cta" to="/docs">
              Get started with Core →
            </Link>
          </div>
          <div className="hero__img">
            <img src={useBaseUrl("/img/hero.png")} />
          </div>
        </div>
<<<<<<< HEAD
        <Image className="hero__image" img={require(`/img/diagrams/datahub-flow-diagram-${colorMode}.png`)} alt="DataHub Flow Diagram" />
        <div className="quickstart__content">
            <h1 className="quickstart__title">Start DataHub Now</h1>
            <p className="quickstart__subtitle">Run the following command to get started with DataHub.</p>
            <div className="quickstart__codeblock">
              <CodeBlock className={"language-shell"}>
                python3 -m pip install --upgrade pip wheel setuptools <br />
                python3 -m pip install --upgrade acryl-datahub <br />
                datahub docker quickstart
              </CodeBlock>
            </div>
              <Link className="button button--primary button--md" to={useBaseUrl("docs/quickstart")}>
              DataHub Quickstart Guide
              </Link>
              <Link className="button button--secondary button--outline button--md" to={useBaseUrl("docs/deploy/kubernetes")}>
              Deploying With Kubernetes
              </Link>
        </div>
        <div className={clsx("card", styles.quickLinks)}>
          <div className={styles.quickLinksLabel}>
            <QuestionCircleOutlined />
            Learn
          </div>
          <Link to={useBaseUrl("docs/introduction")}>What is DataHub?</Link>
          <Link to={useBaseUrl("docs/features")}>What can I do with DataHub?</Link>
          <Link to={useBaseUrl("docs/architecture/architecture")}>How is DataHub architected?</Link>
          <Link to="https://demo.datahubproject.io">See DataHub in action</Link>
=======
        <div className="hero__content_footer">
          Built with&nbsp;<HeartOutlined />&nbsp;by&nbsp;<a href="https://acryldata.io" target="_blank">Acryl Data</a>&nbsp;and&nbsp;<a href="https://www.acryldata.io/press/founded-by-airbnb-and-linkedin-data-veterans-acryl-data-re-imagines-metadata-management-with-dollar9-million-in-seed-funding" target="_blank">LinkedIn</a>.
>>>>>>> upstream/master
        </div>
      </div>
    </header>
  );
};

export default Hero;