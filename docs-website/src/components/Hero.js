import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "../styles/hero.module.scss";

import RoundedImage from "./RoundedImage";

const Hero = ({}) => (
  <header className={clsx("hero", styles.hero)}>
    <div className="container">
      <div className="hero__alert alert alert--primary">
        <span>
          <strong>🎉&nbsp; May 17th &amp; 18th 2020: Metadata Day, Governance as Code.</strong> Join us for expert panel discussions, lightning talks,
          and our inaugural Hackathon!
        </span>

        <Link className="button button--primary button--md" href="http://metadataday.datahubproject.io/" target="_blank">
          RSVP Here →
        </Link>
      </div>

      <div className="row row--centered">
        <div className="col col--5">
          <div className="hero__content">
            <div>
              <h1 className={clsx("hero__title")}>The Metadata Platform for the Modern Data Stack</h1>
              <p className={clsx("hero__subtitle")}>
                Data ecosystems are diverse &#8212; too diverse. DataHub's extensible metadata platform enables data discovery, data observability and
                federated governance that helps you tame this complexity.
              </p>
              <Link className="button button--primary button--lg" to={useBaseUrl("docs/")}>
                Get Started →
              </Link>
              <Link className="button button--secondary button--outline button--lg" to="https://slack.datahubproject.io">
                Join our Slack
              </Link>
            </div>
          </div>
        </div>
        <div className={clsx("col col--6 col--offset-1")}>
          <RoundedImage img={require("/img/screenshots/entity.png")} alt="DataHub Entity Screenshot" />
          <div></div>
        </div>
      </div>
    </div>
  </header>
);

export default Hero;
