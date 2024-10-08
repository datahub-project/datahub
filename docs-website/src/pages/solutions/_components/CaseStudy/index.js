import React from "react";
import styles from "./case-study.module.scss";
import clsx from "clsx";

const CaseStudy = ({ caseStudyContent }) => {
  const { title, items } = caseStudyContent;
  return (
    <div className={styles.container}>
      <div className={styles.case_study}>
        <div className={styles.case_study_heading}>
          {title}
        </div>

        <div className={clsx(styles.card_row, "row")}>
          <div className={styles.card_row_wrapper}>
            {items.map((caseStudy) => (
              <div className={styles.card} key={caseStudy.link}>
                <a
                  className={styles.cardLink}
                  href={caseStudy.link}
                >
                  <div className={styles.card_image}>
                    <img src={caseStudy.imgSrc} alt={caseStudy.alt} />
                  </div>
                  <div className={styles.card_heading_div}>
                    <div className={styles.card_heading}>{caseStudy.title}</div>
                  </div>
                </a>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default CaseStudy;
