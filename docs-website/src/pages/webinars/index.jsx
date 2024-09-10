import React from "react";
import Layout from "@theme/Layout";
import BrowserOnly from "@docusaurus/BrowserOnly";
import WebinarCard from "./_components/WebinarCard";
import styles from "./styles.module.scss";

import webinarsIndexes from "./webinarsIndexes.json";

function WebinarsListPageContent() {
  const webinars = (webinarsIndexes?.webinars || []);

  return (
    <Layout>
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Webinars</h1>
              <p className="hero__subtitle">
                Our live or on-demand webinars can help you gain insights -
                <br />
                from understanding Acryl Data to discovering how businesses leverage it to take back control of their data.
              </p>
            </div>
          </div>
        </div>
      </header>
      <div className="container">
        <div className="row">
          {webinars.map((webinar) => (
            <WebinarCard
              key={webinar.slug}
              slug={webinar.slug}
              title={webinar.title}
              excerpt={webinar.excerpt}
              status={webinar.status}
              cardImg={webinar.cardImg}
              link={webinar.link}
            />
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default function WebinarsListPage() {
  return (
    <WebinarsListPageContent />
  );
}
