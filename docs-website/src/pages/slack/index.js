import React, { useEffect } from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function SlackSurvey() {
    const { siteConfig = {} } = useDocusaurusContext();

    useEffect(() => {
        const script = document.createElement('script');
        script.src = "//js.hsforms.net/forms/embed/v2.js";
        script.async = true;
        script.type = 'text/javascript';
        document.body.appendChild(script);

        script.onload = () => {
            if (window.hbspt) {
                window.hbspt.forms.create({
                    region: "na1",
                    portalId: "14552909",
                    formId: "91357965-a8dc-4e20-875e-5f87e6b9defb",
                    target: '#hubspotForm' // Targeting the div with the specific ID
                });
            }
        };

        return () => {
            document.body.removeChild(script);
        };
    }, []);

    return (
        <Layout
            title={siteConfig.tagline}
            description="Description of the page">
            <header className={"hero"}>
                <div className="container">
                    <div className="hero__content">
                        <h1>Join the DataHub Slack Community!</h1>
                        <div style={{ fontSize: "18px" }}>We'd love to find out a little more about you!</div>
                        <div id="hubspotForm" style={{ width: "90%", maxWidth:"40rem", margin: "3rem auto"}}></div> 
                    </div>
                </div>
            </header>
        </Layout>
    );
}

export default SlackSurvey;
