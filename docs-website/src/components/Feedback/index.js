import React, { useEffect, useState } from "react";
import { Widget } from "@happyreact/react";

import "@happyreact/react/theme.css";

import ExecutionEnvironment from "@docusaurus/ExecutionEnvironment";
import styles from "./styles.module.css";

const VotedYes = () => {
  return <span>Thanks for your feedback. We are glad you like it :)</span>;
};

const VotedNo = () => {
  return <span>Thanks for your feedback. We will try to improve :(</span>;
};

export default function Feedback({ resource }) {
  const [reaction, setReaction] = useState(null);

  const isReacted = reaction === "Yes" || reaction === "No";
  const _resource = String(resource).replace(/\//g, "-");

  const handleReaction = (params) => {
    setReaction(params.icon);
  };

  return (
    <div className={styles.root}>
      <h3 className={styles.title}>Was this page helpful?</h3>
      {!isReacted ? (
        <div className="">
          <Widget
            token="00ce1e55-a9a2-4b59-b0d6-c3c64de7525e"
            resource={_resource}
            classes={{
              root: styles.widget,
              container: styles.container,
              grid: styles.grid,
              cell: styles.cell,
              reaction: styles.reaction,
            }}
            onReaction={handleReaction}
          />
        </div>
      ) : reaction === "No" ? (
        <VotedNo />
      ) : (
        <VotedYes />
      )}
    </div>
  );
}
