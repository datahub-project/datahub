/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import React, { useState, useMemo } from "react";
import styles from "./styles.module.scss";
import { LikeOutlined, DislikeOutlined, CheckCircleOutlined } from "@ant-design/icons";
import { v4 as uuidv4 } from "uuid";

const SlackUtm = ({ pageId }) => {
  return (
    <div className={styles.slackUtm}>
      <div className={styles.slackUtm}>
         <hr />
            Need more help? Join the conversation in <a href={`https://datahub.com/slack?utm_source=docs&utm_medium=footer&utm_campaign=docs_footer&utm_content=${pageId}`}>Slack!</a>
      </div>
    </div>
  );
};

export default SlackUtm;
