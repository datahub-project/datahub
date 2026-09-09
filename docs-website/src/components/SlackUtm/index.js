import React, { useState, useMemo } from "react";
import styles from "./styles.module.scss";
import { LikeOutlined, DislikeOutlined, CheckCircleOutlined } from "@ant-design/icons";
import { v4 as uuidv4 } from "uuid";

const SlackUtm = () => {
  return (
    <div className={styles.slackUtm}>
      <div className={styles.slackUtm}>
         <hr />
            Need more help? Join the conversation in <a href="https://datahub.com/slack">Slack!</a>
      </div>
    </div>
  );
};

export default SlackUtm;
