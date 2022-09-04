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
        Managed DataHub {ossOnly ? <CloseCircleFilled /> : <CheckCircleFilled />}
      </span>
    </div>
  </div>
);

export default FeatureAvailability;
