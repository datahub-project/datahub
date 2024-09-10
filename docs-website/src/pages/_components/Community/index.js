import React from "react";
import styles from "./community.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";

const Community = () => {
  return (
    <div className={styles.container}>
      <div className={styles.community_section}>
        <div className={styles.community_section_left}>
          <div className={styles.community_section_left_content}>
            <div className={styles.community_section_heading}>
              A community that's
              <div>
                <img
                  width={30}
                  height={30}
                  src={useBaseUrl("/img/logos/companies/slack.svg")}
                />
                <span
                  style={{
                    color: "rgba(255,255,255,1)",
                  }}
                >
                  10,332
                </span>{" "}
                strong.
              </div>
            </div>
            <p>
              Q&A.&emsp;Office Hours.&emsp; Monthly Town Hall.&emsp; Job
              Postings.
            </p>
            <a>Join Slack</a>
          </div>
        </div>
        <div className={styles.community_section_right}>
          <div className={styles.community_section_heading}>
            <div>
              With 500+ contributors <br /> world wide.
            </div>
            <a>
              <img width={20} height={20} src={useBaseUrl("/img/github.png")} />
              Open Github
            </a>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Community;
