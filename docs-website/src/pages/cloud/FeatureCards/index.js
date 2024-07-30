import React from "react";
import clsx from "clsx";
import styles from "./styles.module.scss";
import Link from "@docusaurus/Link";
import { CheckCircleOutlined } from "@ant-design/icons";

const data = {
  sections: [
    {
      title: "Search and Discovery",
      icon: "/img/assets/data-discovery.svg",
      cloudBenefits: [
        { text: "Enhanced search ranking →", link: "/" },
        { text: "Personalization for every persona →", link: "/" },
        { text: "A browser extension for BI Tools →", link: "/" },
        { text: "AI-Powered Documentation →", link: "/" },
        { text: "No-Code Automations →", link: "/" },
      ],
      coreBenefits: [
        { text: "Integrations for 50+ data sources"},
        { text: "Lineage for tables, columns, and jobs"},
      ],
      hiddenCoreBenefits: "+4 Benefits",
    },
    {
      title: "Data Governance",
      icon: "/img/assets/data-governance.svg",
      cloudBenefits: [
        { text: "Human-assisted asset certification workflows →", link: "/" },
        { text: "Automations to enforce governance standards →", link: "/" },
        { text: "End-to-end glossary management workflows →", link: "/" },
        { text: "Ownership management workflows →", link: "/" },
      ],
      coreBenefits: [
        { text: "Shift-left governance"},
        { text: "Business glossaries"},
      ],
      hiddenCoreBenefits: "+1 Benefit",
    },
    {
      title: "Data Observability",
      icon: "/img/assets/data-ob.svg",
      cloudBenefits: [
        { text: "Continue data quality monitors →", link: "/" },
        { text: "End-to-end data incident tracking & management →", link: "/" },
        { text: "AI-Driven anomaly detection →", link: "/" },
        { text: "Executive Data Health Dashboard →", link: "/" },
        { text: "On-demand data quality evaluation via APIs & UI →", link: "/" },
      ],
      coreBenefits: [
        { text: "Surface data quality results"},
        { text: "Create and manage data contracts" },
      ],
      hiddenCoreBenefits: "+1 Benefit",
    },
  ],
};

const Features = () => (
  <div className="container">
    {data.sections.map((section, sectionIndex) => (
      <div
        key={sectionIndex}
        className={clsx("row row--padded row--centered", {
          [styles.reversedRow]: sectionIndex % 2 === 1,
        })}
      >
        <div className={clsx(styles.cardGroup, {
          [styles.cardGroupInverse]: section.title === "Data Governance"
        })}>
          <div className={clsx("col col--6", styles.cloudCard)}>
            <div className={clsx(styles.titleContainer)}>
              <div className={clsx(styles.title)}>
                <img
                  className={clsx(styles.icon)}
                  src={section.icon}
                  alt={section.title}
                />
                <div className={clsx(styles.titleText)}>{section.title}</div>
              </div>
            </div>
            <div className={clsx(styles.card, styles.cloudBenefitCard)}>
              <div className={clsx(styles.sectionTitle)}>DataHub Cloud includes:</div>
              <div className={clsx(styles.cloudBenefitList, styles.featureList)}>
                {section.cloudBenefits.map((benefit, index) => ( 
                  <div key={index} className={clsx(styles.cloudBenefit)} >
                    <Link to={benefit.link}>
                      <CheckCircleOutlined className={clsx(styles.benefitIcon)} /> {benefit.text}
                    </Link>
                  </div>
                ))}
              </div>
              <Link
                className={clsx("button button--primary button--md", styles.exploreButton)}
                to="/"
              >
                Explore in DataHub Cloud
              </Link> 
            </div>
          </div>
          <div className={clsx("col col--6", styles.coreCard)}>
            <div className={clsx(styles.card, styles.coreBenefitCard)}>
              <h3>{`In addition to DataHub Core`}</h3>
              <div className={clsx(styles.featureList)}>
                {section.coreBenefits.map((benefit, index) => (
                  <div key={index} className={clsx(styles.coreBenefit)}>
                    <CheckCircleOutlined className={clsx(styles.benefitIcon)} /> {benefit.text}
                  </div>
                ))}
              </div>
              <div className={clsx(styles.moreBenefits)}>{section.hiddenCoreBenefits}</div>
            </div>
          </div>
        </div>
      </div>
    ))}
  </div>
);

export default Features;
