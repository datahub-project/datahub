import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Image from "@theme/IdealImage";
import { useColorMode } from "@docusaurus/theme-common";
import { QuestionCircleOutlined } from "@ant-design/icons";
import styles from "./hero.module.scss";
import CodeBlock from "@theme/CodeBlock";
import TownhallButton from "../TownhallButton";
import { Section } from "../Section";

const HeroAnnouncement = ({ message, linkUrl, linkText }) => (
  <div className={clsx("hero__alert alert alert--primary", styles.hero__alert)}>
    <span>{message}</span>
    {linkUrl && (
      <Link className="button button--primary button--md" href={linkUrl} target="_blank">
        {linkText}
      </Link>
    )}
  </div>
);

const Hero = ({}) => {
  const { colorMode } = useColorMode();
  return (
    <header className={clsx("hero", styles.hero)}>
      <div className="container">
        {/* HeroAnnouncement goes here */}
        <div className="hero__content">
          <div>
            <h1 className="hero__title">The #1 Open Source Metadata Platform</h1>
            <p className="hero__subtitle">
              DataHub is an extensible data catalog that enables data discovery, data observability and federated governance to help tame the
              complexity of your data ecosystem.
            </p>
            <p className="hero__subtitle">
              Built with ❤️ by <img src={useBaseUrl("/img/acryl-logo-transparent-mark.svg")} width="25" />{" "}
              <a href="https://acryldata.io" target="blank" rel="noopener noreferrer">
                Acryl Data
              </a>{" "}
              and <img src={useBaseUrl("img/LI-In-Bug.png")} width="25" /> LinkedIn.
            </p>
            <Link className="button button--primary button--md" to={useBaseUrl("docs/")}>
              Get Started →
            </Link>
            <Link className="button button--secondary button--md" to="https://slack.datahubproject.io">
              Join our Slack
            </Link>
            <TownhallButton />
          </div>
        </div>
      </div>
    </header>
  );
};

export default Hero;
