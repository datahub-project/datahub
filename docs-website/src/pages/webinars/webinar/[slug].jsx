import React, { useEffect, useState } from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import webinarsIndexes from "../webinarsIndexes.json";
import styles from "./styles.module.css";
import { useParams } from "@docusaurus/router";

function GatedWebinar() {

  const { siteConfig = {} } = useDocusaurusContext();
  const [isFormSubmitted, setIsFormSubmitted] = useState(false);
  const slug = "why-data-quality-belongs-in-the-data-catalog";

  const webinar = webinarsIndexes.webinars.find((webinar) => webinar.slug === slug);

  console.log("Webinar data: ", webinar);
  console.log("Slug parameter: ", slug);
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
    return <Layout><p>Webinar not found.</p></Layout>;
  }

  return (
    <Layout>
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <h1>Access the Exclusive Webinar: {webinar.title}</h1>
            <div style={{ fontSize: "18px" }}>Enter your email to access the webinar.</div>

            {/* HubSpot Form */}
            {!isFormSubmitted && (
              <div id="hubspotGateForm" style={{ width: "90%", maxWidth: "40rem", margin: "3rem auto" }}></div>
            )}

            {/* Webinar Content */}
            {isFormSubmitted && (
              <div style={{ width: "90%", maxWidth: "40rem", margin: "3rem auto" }}>
                <div style={{ position: "relative", paddingBottom: "64.5933014354067%", height: 0 }}>
                  <iframe
                    src={webinar.webinarSrc}
                    frameBorder="0"
                    webkitAllowFullScreen
                    mozAllowFullScreen
                    allowFullScreen
                    style={{ position: "absolute", top: 0, left: 0, width: "100%", height: "100%" }}
                  ></iframe>
                </div>
                <div>
                  <h2>Presenter: {webinar.presenterName}</h2>
                  <p>{webinar.presenterTitle}</p>
                  {webinar.presenterImg && <img src={webinar.presenterImg} alt={webinar.presenterName} />}
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
