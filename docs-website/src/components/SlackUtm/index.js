import React, { useState, useMemo } from "react";
import clsx from "clsx";
import { supabase } from "./supabase";
import styles from "./styles.module.scss";
import { LikeOutlined, DislikeOutlined, CheckCircleOutlined } from "@ant-design/icons";
import { v4 as uuidv4 } from "uuid";

const SlackUtm = ({ pageId }) => {
  return (
    <div className={styles.slackUtm}>
      <div className={styles.slackUtm}>
         <hr />
         <b>
            Need more help? Join the conversation in <a href={`https://datahubproject.io/slack?utm_source=docs&utm_medium=footer&utm_campaign=docs_footer&utm_content=${pageId}`}>Slack!</a>
         </b>
      </div>
    </div>
  );
};

export default SlackUtm;
