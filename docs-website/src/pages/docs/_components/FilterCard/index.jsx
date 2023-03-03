import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcard.module.scss";

import {
  ThunderboltTwoTone,
  ApiTwoTone,
  DeploymentUnitOutlined,
  SyncOutlined,
  CodeTwoTone,
  QuestionCircleTwoTone,
  SlidersTwoTone,
  HeartTwoTone,
} from "@ant-design/icons";

const FilterCard = ({ icon, platformIcon, title, description, to }) => {
  return (
    <div className="col col--4">
      <Link to={useBaseUrl(to)} className={clsx("card", styles.feature)}>
        <div>
          <div style={{ display: "flex", alignItems: "center" }}>
            {platformIcon && (
              <div style={{ display: "flex", alignItems: "center" }}>
                <img src={useBaseUrl(platformIcon)} />
              </div>
            )}
            <strong>{title}</strong>
          </div>
          <span style={{ marginLeft: "1rem" }}>{description}</span>
        </div>
      </Link>
    </div>
  );
};

export default FilterCard;
