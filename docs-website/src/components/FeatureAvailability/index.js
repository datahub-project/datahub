import React from "react";
import clsx from "clsx";
import styles from "./featureavailability.module.scss";
import {
  CheckCircleFilled,
  CloseCircleFilled,
  CloudOutlined,
  ExperimentOutlined,
  MinusCircleFilled,
} from "@ant-design/icons";
import { STAGE_META } from "./stages";

const renderSelfHostedIcon = (saasOnly, selfHostedPartial) => {
  if (saasOnly) return <CloseCircleFilled />;
  if (selfHostedPartial) return <MinusCircleFilled />;
  return <CheckCircleFilled />;
};

const FeatureAvailability = ({
  saasOnly,
  ossOnly,
  stage,
  selfHostedPartial,
  comparisonLink,
}) => {
  const stageMeta = stage ? STAGE_META[stage] : null;
  const selfHostedAvailable = !saasOnly && !selfHostedPartial;
  return (
    <div className={clsx(styles.availabilityCard, "card")}>
      <strong>Feature Availability</strong>
      <div>
        <span
          className={clsx(
            styles.platform,
            selfHostedAvailable && styles.platformAvailable,
            selfHostedPartial && styles.platformPartial
          )}
        >
          Self-Hosted DataHub{" "}
          {renderSelfHostedIcon(saasOnly, selfHostedPartial)}
        </span>
      </div>
      <div>
        <CloudOutlined className={styles.managedIcon} />
        <span
          className={clsx(
            styles.platform,
            !ossOnly && styles.platformAvailable
          )}
        >
          DataHub Cloud{" "}
          {ossOnly ? <CloseCircleFilled /> : <CheckCircleFilled />}
        </span>
      </div>
      {comparisonLink && (
        <div className={styles.comparisonLinkWrapper}>
          <a href={comparisonLink} className={styles.comparisonLink}>
            Compare OSS vs Cloud →
          </a>
        </div>
      )}
      {stageMeta && (
        <div className={styles.stageWrapper}>
          <span
            className={clsx(styles.stageBadge, styles[stageMeta.className])}
          >
            {stage !== "ga" && (
              <ExperimentOutlined className={styles.stageIcon} />
            )}
            {stageMeta.label}
          </span>
          <span className={styles.stageDescription}>
            {stageMeta.description}
          </span>
        </div>
      )}
    </div>
  );
};

export default FeatureAvailability;
