import React, { useEffect, useState } from "react";
import { v4 as uuidv4 } from "uuid";

const makeHubspotForm = (formId, uniqueId) => {
  window.hbspt.forms.create({
    region: "na1",
    portalId: "14552909",
    formId: formId,
    target: `#form-${uniqueId}`,
    formInstanceId: `instance-${uniqueId}`,
  });
};

const loadHubspotScript = (formId, uniqueId) => {
  let script = document.createElement(`script`);
  script.defer = true;
  script.onload = () => {
    makeHubspotForm(formId, uniqueId);
  };
  script.src = `//js.hsforms.net/forms/v2.js`;
  document.head.appendChild(script);
};

const HubspotForm = ({ formId }) => {
  const [uniqueId, setUniqueID] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setUniqueID(uuidv4());
  }, []);

  useEffect(() => {
    if (uniqueId) {
      if (!window.hbspt) {
        loadHubspotScript(formId, uniqueId);
      } else {
        makeHubspotForm(formId, uniqueId);
      }
      setLoading(false);
    }
  }, [uniqueId]);
  return <>{loading ? <>Loading...</> : <div id={`form-${uniqueId}`}></div>}</>;
};

export default HubspotForm;
