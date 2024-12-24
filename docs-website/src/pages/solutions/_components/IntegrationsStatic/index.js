import React, { useRef, useEffect } from "react";
import styles from "./integrations.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";

const Integrations = () => {
  const integrationsPath = 'img/solutions/integrations-observe';


  return (
    <div className={styles.container}>
      <div className={styles.section_header}>
          <span>Integrates with your data stack</span>
      </div>
      <div className={styles.community_section}>
            <div className={styles.carouselContainer}>
              <div className={styles.slider}>
                <div className={styles.slide_track}>
                  {[...Array(1)].map((_, i) => (
                    <React.Fragment key={i}>
                      {[1, 2, 3, 4, 5, 6, 7, 8].map((item, index) => (
                        <div className={styles.slide} key={index} style={{ backgroundImage: `url(${useBaseUrl(`${integrationsPath}/logo-integration-${item}.png`)})` }}>
                        </div>
                      ))}
                    </React.Fragment>
                  ))}
                </div>
              </div>
          </div>
      </div>
    </div>
  );
};

export default Integrations;
