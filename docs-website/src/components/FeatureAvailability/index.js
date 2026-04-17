import React from "react";
import clsx from "clsx";
import styles from "./featureavailability.module.scss";
import { CheckCircleFilled, CloseCircleFilled, CloudOutlined, ExperimentOutlined } from "@ant-design/icons";

const STAGE_META = {
  alpha: {
    label: "Alpha",
    className: "stageAlpha",
    description:
      "Proof-of-concept, limited to a small set of accounts and built directly with our SE/FDE team. Expect high-touch collaboration and rapid iteration.",
  },
  "private-beta": {
    label: "Private Beta",
    className: "stagePrivateBeta",
    description:
      "Invite-only and evolving rapidly. Contact your DataHub Cloud representative to request access.",
  },
  "public-beta": {
    label: "Public Beta",
    className: "stagePublicBeta",
    description:
      "Available to all DataHub Cloud customers. Mostly stable with minor iteration; we welcome feedback on UX and bugs.",
  },
  ga: {
    label: "Generally Available",
    className: "stageGa",
    description: "Production-ready and fully supported.",
  },
  deprecated: {
    label: "Deprecated",
    className: "stageDeprecated",
    description:
      "This feature is no longer being developed. It will continue to function but will be removed in a future release.",
  },
};

const FeatureAvailability = ({ saasOnly, ossOnly, stage }) => {
  const stageMeta = stage ? STAGE_META[stage] : null;
  return (
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
      {stageMeta && (
        <div className={styles.stageWrapper}>
          <span className={clsx(styles.stageBadge, styles[stageMeta.className])}>
            {stage !== "ga" && <ExperimentOutlined className={styles.stageIcon} />}
            {stageMeta.label}
          </span>
          <span className={styles.stageDescription}>{stageMeta.description}</span>
        </div>
      )}
    </div>
  );
};

export default FeatureAvailability;
