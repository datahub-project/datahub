import React from "react";
import { useEffect, useState } from "react";
import styles from "./styles.module.scss";
import { v4 as uuidv4 } from "uuid";

const makeHubspotForm = (formId, uniqueId, inlineMessage) => {
  const config = {
    region: "na1",
    portalId: "14552909",
    formId: formId,
    target: `#form-${uniqueId}`,
    formInstanceId: `instance-${uniqueId}`,
    inlineMessage: inlineMessage,
  };

  window.hbspt.forms.create(config);
};

const loadHubspotScript = (formId, uniqueId, inlineMessage) => {
  const script = document.createElement(`script`);
  script.defer = true;
  script.onload = () => {
    makeHubspotForm(formId, uniqueId, inlineMessage);
  };
  script.src = `//js.hsforms.net/forms/v2.js`;
  document.head.appendChild(script);
};

const HubspotForm = ({ formId, inlineMessage, ...props }) => {
  const [uniqueId] = useState(uuidv4());
  const [loading, setLoading] = useState(true);
  const [isHubspotLoaded, setIsHubspotLoaded] = useState(false);

  useEffect(() => {
    if (uniqueId) {
      if (!window.hbspt) {
        loadHubspotScript(formId, uniqueId, inlineMessage);
      } else {
        makeHubspotForm(formId, uniqueId, inlineMessage);
      }
      setLoading(false);
    }
  }, [uniqueId]);

  useEffect(() => {
    window.addEventListener("message", (ev) => {
      if (
        ev.data.type === "hsFormCallback" &&
        ev.data.eventName === "onFormReady"
      ) {
        setIsHubspotLoaded(true);
      }
    });
  }, [loading]);

  return (
    <div id={`form-${uniqueId}`} className={styles.hubspotForm} {...props}>
      Loading...
    </div>
  );
};

export default HubspotForm;
