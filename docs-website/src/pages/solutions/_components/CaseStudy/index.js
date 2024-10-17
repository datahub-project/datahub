import React from "react";
import styles from "./case-study.module.scss";
import clsx from "clsx";
import Link from '@docusaurus/Link'

const CaseStudy = ({ caseStudyContent }) => {
  const { title, backgroundColor, items } = caseStudyContent;
  return (
    <div className={styles.container} style={{ backgroundColor: backgroundColor }}>
      <div className={styles.case_study}>
        <div className={styles.case_study_heading}>
          {title}
        </div>

        <div className={clsx(styles.card_row)}>
          <div className={styles.card_row_wrapper}>
            {items.map((caseStudy) => (
              <Link className={clsx(styles.card, "row")} key={caseStudy.link} to={caseStudy.link}>
                  <div className={clsx(styles.card_image, "col col--3")}>
                    <img src={caseStudy.imgSrc} alt={caseStudy.alt} />
                  </div>
                  <div className={clsx(styles.card_heading_div, "col col--8")}>
                    <div className={styles.card_heading}>{caseStudy.title}</div>
                  </div>
              </Link>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default CaseStudy;
