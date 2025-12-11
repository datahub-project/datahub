/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import React from "react";
import clsx from "clsx";
import styles from "./featurecard.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";

const FeatureCard = ({icon, title, description, to}) => {
return (
    <div className="col col--4">
      <Link to={useBaseUrl(to)} className={clsx("card", styles.feature)}>
        <div className={styles.card_content}>
          {icon}
          <strong>{title}&nbsp;→</strong>
          <span>{description}</span>
        </div>
      </Link>
    </div>
  );
};

export default FeatureCard;
