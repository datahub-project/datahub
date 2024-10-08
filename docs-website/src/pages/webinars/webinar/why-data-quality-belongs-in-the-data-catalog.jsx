import React, { useEffect, useState } from "react";
import Layout from "@theme/Layout";
import webinarsIndexes from "../webinarsIndexes.json";
import clsx from "clsx";
import styles from "./styles.module.scss";
import "./hubspotFormStyle.css"

function GatedWebinar() {
  const [isFormSubmitted, setIsFormSubmitted] = useState(false);
  const slug = "why-data-quality-belongs-in-the-data-catalog";

  const webinar = webinarsIndexes.webinars.find((webinar) => webinar.slug === slug);

  useEffect(() => {
    if (!webinar) return;

    const script = document.createElement("script");
    script.src = "//js.hsforms.net/forms/embed/v2.js";
    script.async = true;
    script.type = "text/javascript";
    document.body.appendChild(script);

    script.onload = () => {
      if (window.hbspt) {
        window.hbspt.forms.create({
          region: "na1",
          portalId: "14552909",
          formId: webinar.gateFormId, // Use form ID from webinar data
          target: "#hubspotGateForm",
          onFormSubmit: () => {
            setIsFormSubmitted(true);
          },
        });
      }
    };

    return () => {
      document.body.removeChild(script);
    };
  }, [webinar]);

  if (!webinar) {
    return (
      <Layout>
        <p>Webinar not found.</p>
      </Layout>
    );
  }

  return (
    <Layout>
      <header className={clsx("hero", styles.hero)}>
        <div className={clsx("container", styles.container)}>
          <div className={clsx("hero__content", styles.heroContent)}>
            <h1>{webinar.title}</h1>
            {/* HubSpot Form */}
            {!isFormSubmitted && (
                <>
                <div id="hubspotGateForm" className={clsx(styles.hubspotGateForm)}></div>
                <div className={clsx(styles.contentPreview)}>
                  <p>{webinar.content}</p>
                </div>
              </>
            )}

            {/* Webinar Content */}
            {isFormSubmitted && (
              <div className={clsx(styles.webinarContent)}>
                <div className={clsx(styles.videoContainer)}>
                  <iframe
                    src={webinar.webinarSrc}
                    frameBorder="0"
                    webkitAllowFullScreen
                    mozAllowFullScreen
                    allowFullScreen
                    className={clsx(styles.webinarIframe)}
                  ></iframe>
                </div>
                <div className={clsx(styles.presenterSection)}>
                  <h2>Presenter: {webinar.presenterName}</h2>
                  <p>{webinar.presenterTitle}</p>
                  {webinar.presenterImg && (
                    <img
                      src={webinar.presenterImg}
                      alt={webinar.presenterName}
                      className={clsx(styles.presenterImg)}
                    />
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </header>
    </Layout>
  );
}

export default GatedWebinar;
