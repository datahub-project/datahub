import React, { useContext, useEffect, useState } from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { usePluginData } from "@docusaurus/useGlobalData";
import styles from "./markprompthelp.module.scss";
import * as Markprompt from "@markprompt/react";
import { VisuallyHidden } from "@radix-ui/react-visually-hidden";

import { LikeOutlined, DislikeOutlined } from "@ant-design/icons";

const MarkpromptHelp = () => {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Markprompt.Root projectKey={siteConfig.customFields.markpromptProjectKey} model="gpt-4">
      <Markprompt.Trigger aria-label="Open Markprompt" className={styles.MarkpromptButton}>
        <BotIcon /> Ask our AI
      </Markprompt.Trigger>
      <Markprompt.Portal>
        <Markprompt.Overlay className={styles.MarkpromptOverlay} />
        <Markprompt.Content className={styles.MarkpromptContent}>
          <Markprompt.Close className={styles.MarkpromptClose}>
            <CloseIcon />
          </Markprompt.Close>
          {/* Markprompt.Title is required for accessibility reasons. It can be hidden using an accessible content hiding technique. */}
          <VisuallyHidden asChild>
            <Markprompt.Title>Ask me anything about DataHub!</Markprompt.Title>
          </VisuallyHidden>
          <Markprompt.Form>
            <SearchIcon className={styles.MarkpromptSearchIcon} />
            <Markprompt.Prompt placeholder="Ask me anything about DataHub!" className={styles.MarkpromptPrompt} />
          </Markprompt.Form>
          <Markprompt.AutoScroller className={styles.MarkpromptAnswer}>
            <Caret />
            <Markprompt.Answer />
          </Markprompt.AutoScroller>
          <References />
          <div className={styles.MarkpromptDescription}>
            This is an experimental AI-powered chat bot. We can't be sure what it will say but hope it can be helpful. If it's not, there are always{" "}
            <a href="https://slack.datahubproject.io/" target="_blank">
              humans available in our Slack channel.
            </a>
          </div>
          <Feedback />
        </Markprompt.Content>
      </Markprompt.Portal>
    </Markprompt.Root>
  );
};

async function sendFeedback(data = {}) {
  const response = await fetch("https://hlmqfkovdugtwoddrtkt.supabase.co/rest/v1/queries", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Prefer: "return=minimal",
      apikey:
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhsbXFma292ZHVndHdvZGRydGt0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODI5NzQwMjQsImV4cCI6MTk5ODU1MDAyNH0.wNggN7IK43j6KhJmFlELo8HmkTBIQwWt-Rx8vOYXBI8",
    },
    redirect: "follow",
    referrerPolicy: "no-referrer",
    body: JSON.stringify(data),
  });
  return response;
}

const Feedback = () => {
  const { state, prompt } = useContext(Markprompt.Context);
  const [feedbackSubmitted, setFeedbackSubmitted] = useState(false);

  useEffect(() => {
    setFeedbackSubmitted(false);
    if (state === "preload") {
      sendFeedback({
        query: prompt,
      }).then((response) => {
        console.log(response.ok ? "Logged query" : "Error logging query");
      });
    }
  }, [state]);

  const handleClick = (rating) => {
    sendFeedback({
      query: prompt,
      result_rating: rating,
    }).then((response) => {
      setFeedbackSubmitted(true);
      console.log(response.ok ? "Logged feedback" : "Error logging feedback");
    });
  };

  return (
    <div className={styles.feedback} data-loading-state={state}>
      {feedbackSubmitted ? (
        <>Thanks for your feedback!</>
      ) : (
        <>
          Was this answer helpful?{" "}
          <div className={styles.feedbackButtons}>
            <button onClick={() => handleClick("positive")}>
              <LikeOutlined />
            </button>
            <button>
              <DislikeOutlined onClick={() => handleClick("negative")} />
            </button>
          </div>
        </>
      )}
    </div>
  );
};

const Caret = () => {
  const { answer } = useContext(Markprompt.Context);

  if (answer) {
    return null;
  }

  return <span className={styles.caret} />;
};

const getReferenceInfo = (referenceId) => {
  const contentDocsData = usePluginData("docusaurus-plugin-content-docs");
  const docs = contentDocsData.versions?.[0]?.docs;

  const lastDotIndex = referenceId.lastIndexOf(".");
  if (lastDotIndex !== -1) {
    referenceId = referenceId.substring(0, lastDotIndex);
  }
  const docItem = docs?.find((doc) => doc.id === referenceId);

  const url = docItem?.path;
  const title = url?.replace("/docs/generated/", "")?.replace("/docs/", "");

  return { url, title };
};

const Reference = ({ referenceId, index }) => {
  const { title, url } = getReferenceInfo(referenceId);

  if (!url) return null;

  return (
    <li
      key={referenceId}
      className={styles.reference}
      style={{
        animationDelay: `${100 * index}ms`,
      }}
    >
      <a href={url} target="_blank">
        {title || url}
      </a>
    </li>
  );
};

const References = () => {
  const { state, references } = useContext(Markprompt.Context);

  if (state === "indeterminate") return null;

  let adjustedState = state;
  if (state === "done" && references.length === 0) {
    adjustedState = "indeterminate";
  }

  return (
    <div data-loading-state={adjustedState} className={styles.references}>
      <div className={styles.progress} />
      <p>Fetching relevant pages…</p>
      <p>Answer generated from the following sources:</p>
      <Markprompt.References RootElement="ul" ReferenceElement={Reference} />
    </div>
  );
};

const SearchIcon = ({ className }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className={className}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <circle cx="11" cy="11" r="8"></circle>
    <line x1="21" x2="16.65" y1="21" y2="16.65"></line>
  </svg>
);

const CloseIcon = ({ className }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className={className}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <line x1="18" x2="6" y1="6" y2="18"></line>
    <line x1="6" x2="18" y1="6" y2="18"></line>
  </svg>
);

const BotIcon = ({ ...props }) => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 87.5 79.164" {...props}>
    <path d="M64.582 8.332h-18.75v-6.25C45.832.93 44.902 0 43.75 0s-2.082.93-2.082 2.082v6.25h-18.75c-5.754.004-10.414 4.664-10.418 10.418v37.5c.004 5.75 4.664 10.41 10.418 10.414H34.3l7.715 11.574a2.089 2.089 0 0 0 3.468 0L53.2 66.664h11.383C70.336 66.66 74.996 62 75 56.25v-37.5c-.004-5.754-4.664-10.414-10.418-10.418Zm6.25 47.918a6.256 6.256 0 0 1-6.25 6.25h-12.5c-.695 0-1.344.348-1.73.926l-6.602 9.902-6.602-9.898v-.004a2.082 2.082 0 0 0-1.73-.926h-12.5a6.256 6.256 0 0 1-6.25-6.25V50h54.168l-.004 6.25Zm0-10.418H16.668V18.75a6.253 6.253 0 0 1 6.25-6.25h41.664a6.253 6.253 0 0 1 6.25 6.25v27.082ZM35.418 31.25a4.165 4.165 0 0 1-2.574 3.848 4.166 4.166 0 1 1-1.594-8.016c1.105 0 2.164.438 2.945 1.219s1.223 1.844 1.223 2.95v-.001Zm25 0a4.165 4.165 0 0 1-2.574 3.848 4.166 4.166 0 1 1-1.594-8.016c1.105 0 2.164.438 2.945 1.219s1.223 1.844 1.223 2.95v-.001Zm-56.25-8.336v16.668a2.082 2.082 0 0 1-2.086 2.082A2.081 2.081 0 0 1 0 39.582V22.914a2.084 2.084 0 0 1 4.168 0Zm83.332 0v16.668a2.081 2.081 0 0 1-2.082 2.082 2.082 2.082 0 0 1-2.086-2.082V22.914a2.085 2.085 0 0 1 4.168 0Z" />
  </svg>
);

export default MarkpromptHelp;
