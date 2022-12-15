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




const QuickLinkCard = ({ icon, platformIcon, title, description, to }) => {
  return (
    <div className="col col--3">
      <Link to={useBaseUrl(to)} className={clsx("card", styles.feature)}>
        {icon}
        {platformIcon && <img src={useBaseUrl(`/img/logos/platforms/${platformIcon}.svg`)} />}
        <div>
          <strong>{title}</strong>
          <span>{description}</span>
        </div>
      </Link>
    </div>
  );
};

export default QuickLinkCard;
