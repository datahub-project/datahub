import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { Markprompt } from "markprompt";
import clsx from "clsx";
import styles from "./markprompthelp.module.scss";
import { ModalProvider, useModal } from "react-modal-hook";
import Modal from "react-modal";

const BotIcon = ({ ...props }) => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 87.5 79.164" {...props}>
    <path d="M64.582 8.332h-18.75v-6.25C45.832.93 44.902 0 43.75 0s-2.082.93-2.082 2.082v6.25h-18.75c-5.754.004-10.414 4.664-10.418 10.418v37.5c.004 5.75 4.664 10.41 10.418 10.414H34.3l7.715 11.574a2.089 2.089 0 0 0 3.468 0L53.2 66.664h11.383C70.336 66.66 74.996 62 75 56.25v-37.5c-.004-5.754-4.664-10.414-10.418-10.418Zm6.25 47.918a6.256 6.256 0 0 1-6.25 6.25h-12.5c-.695 0-1.344.348-1.73.926l-6.602 9.902-6.602-9.898v-.004a2.082 2.082 0 0 0-1.73-.926h-12.5a6.256 6.256 0 0 1-6.25-6.25V50h54.168l-.004 6.25Zm0-10.418H16.668V18.75a6.253 6.253 0 0 1 6.25-6.25h41.664a6.253 6.253 0 0 1 6.25 6.25v27.082ZM35.418 31.25a4.165 4.165 0 0 1-2.574 3.848 4.166 4.166 0 1 1-1.594-8.016c1.105 0 2.164.438 2.945 1.219s1.223 1.844 1.223 2.95v-.001Zm25 0a4.165 4.165 0 0 1-2.574 3.848 4.166 4.166 0 1 1-1.594-8.016c1.105 0 2.164.438 2.945 1.219s1.223 1.844 1.223 2.95v-.001Zm-56.25-8.336v16.668a2.082 2.082 0 0 1-2.086 2.082A2.081 2.081 0 0 1 0 39.582V22.914a2.084 2.084 0 0 1 4.168 0Zm83.332 0v16.668a2.081 2.081 0 0 1-2.082 2.082 2.082 2.082 0 0 1-2.086-2.082V22.914a2.085 2.085 0 0 1 4.168 0Z" />
  </svg>
);

const MarkpromptModal = () => {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  const [showModal, hideModal] = useModal(() => (
    <Modal isOpen onRequestClose={hideModal} className={clsx(styles.markpromptModal, "shadow--tl")} ariaHideApp={false}>
      <>
        <div className={clsx(styles.markprompt)}>
          <BotIcon />
          <Markprompt placeholder="Ask me anything about DataHub!" projectKey={siteConfig.customFields.markpromptProjectKey} />
        </div>
        <hr />
        <p>
          This is an experimental AI-powered chat bot. We can't be sure what it will say but hope it can be helpful. If it's not, there are always{" "}
          <a href="https://slack.datahubproject.io/" target="_blank">
            humans available in our Slack channel
          </a>
          .
        </p>
      </>
    </Modal>
  ));

  return (
    <div>
      <button className={clsx(styles.markpromptButton, "button button--primary shadow--tl")} onClick={showModal}>
        <BotIcon />
      </button>
    </div>
  );
};

const MarkpromptHelp = () => (
  <ModalProvider>
    <MarkpromptModal />
  </ModalProvider>
);

export default MarkpromptHelp;
