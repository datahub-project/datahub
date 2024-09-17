import React from "react";
import styles from "./case-study.module.scss";
import Link from '@docusaurus/Link'
import { Carousel } from "antd";
import caseStudyData from "./caseStudyContent";

const contentStyle = {
  height: "80%",
  width: "100%",
};

const CaseStudy = () => {
  const detectDeviceType = () =>
    /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(
      navigator.userAgent
    )
      ? "Mobile"
      : "Desktop";
  return (
    <div className={styles.container}>
      {/* Section-1 */}
      <div className={styles.case_study}>
        <div className={styles.case_study_heading}>
          <div>See how Industry leaders use the Datahub</div>
          <p>Across finance, healthcare, e-commerce and countless more.</p>
        </div>

        <div className={styles.card_row}>
          <Carousel
            slidesToShow={detectDeviceType() === "Desktop" ? 4 : 1}
            initialSlide={0}
            draggable
            style={contentStyle}
          >
            {caseStudyData.map((caseStudy, idx) => (
              <div className={styles.card} key={idx}>
                <a className={styles.cardLink} href={caseStudy.link}>
                  <span className={styles.card_tag}>{caseStudy.tag}</span>
                  <img src={caseStudy.backgroundImage} alt="" />
                  <div className={styles.card_heading_div}>
                    <div className={styles.card_heading}>
                      <span>{caseStudy.title}</span>
                    </div>
                    <div
                      className={styles.card_para}
                      dangerouslySetInnerHTML={{
                        __html: caseStudy.description,
                      }}
                    />
                  </div>
                </a>
              </div>
            ))}
          </Carousel>
        </div>
        <Link className={styles.bottom_line} to="/adoption-stories">
          See all adoption stories →
        </Link>
      </div>
    </div>
  );
};

export default CaseStudy;
