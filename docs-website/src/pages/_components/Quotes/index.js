import React from "react";
import clsx from "clsx";
import styles from "./quotes.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";

const quotesContent = [
  {
    quote: `“[DataHub] has made our legal team very happy with being able to keep track of our sensitive data [to answer questions like] Where’s it going? How’s it being processed? Where’s it ending up? Which third party tool or API’s are we sending it to and why? Who is responsible for this integration?”`,
    company: {
      name: "Wolt",
      imageUrl: "/img/logos/companies/wolt.png",
      imageSize: "default",
    },
  },
  {
    quote: `“DataHub aligns with our needs [for] data documentation, a unified search experience, lineage information, and additional metadata. We are also very impressed with the vibrant and supportive community.”`,
    company: {
      name: "Coursera",
      imageUrl: "/img/logos/companies/coursera.svg",
      imageSize: "small",
    },
  },
  {
    quote: `“DataHub allows us to solve the data discovery problem, which was a big challenge in our organization, and now we are solving it.”`,
    company: {
      name: "Adevinta",
      imageUrl: "/img/logos/companies/adevinta.png",
      imageSize: "small",
    },
  },
];

const Quote = ({ quote, company }) => {
  return (
    <div className="col col--4" style={{ paddingLeft: "0", paddingRight: "2rem" }}>
      <div className={clsx("card", styles.quote)}>
        {quote}
        <div className={styles.companyLogoWrapper}>
          <img
            src={useBaseUrl(company.imageUrl)}
            alt={company.name}
            title={company.name}
            className={clsx(styles.companyLogo, styles[company.imageSize])}
          />
        </div>
      </div>
    </div>
  );
};

const Quotes = () =>
  quotesContent?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row">
          {quotesContent.map((props, idx) => (
            <Quote key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default Quotes;
