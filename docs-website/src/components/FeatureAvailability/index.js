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
import styles from "./featureavailability.module.scss";
import { CheckCircleFilled, CloseCircleFilled, CloudOutlined } from "@ant-design/icons";

const FeatureAvailability = ({ saasOnly, ossOnly }) => (
  <div className={clsx(styles.availabilityCard, "card")}>
    <strong>Feature Availability</strong>
    <div>
      <span className={clsx(styles.platform, !saasOnly && styles.platformAvailable)}>
        Self-Hosted DataHub {saasOnly ? <CloseCircleFilled /> : <CheckCircleFilled />}
      </span>
    </div>
    <div>
      <CloudOutlined className={styles.managedIcon} />
      <span className={clsx(styles.platform, !ossOnly && styles.platformAvailable)}>
        DataHub Cloud {ossOnly ? <CloseCircleFilled /> : <CheckCircleFilled />}
      </span>
    </div>
  </div>
);

export default FeatureAvailability;
