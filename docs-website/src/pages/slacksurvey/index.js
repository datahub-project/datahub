import React, { useEffect, useRef } from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function SlackSurvey() {
    const { siteConfig = {} } = useDocusaurusContext();
    const scriptContainerRef = useRef(null);

    useEffect(() => {
        if (scriptContainerRef.current) {
            const script = document.createElement('script');
            script.src = '//js.hsforms.net/forms/embed/v2.js';
            script.async = true;
            script.type = 'text/javascript';
            script.onload = () => {
                if (window.hbspt) {
                    window.hbspt.forms.create({
                        region: 'na1',
                        portalId: '14552909',
                        formId: '91357965-a8dc-4e20-875e-5f87e6b9defb',
                    });
                }
            };

            scriptContainerRef.current.appendChild(script);

            return () => {
                if (scriptContainerRef.current) {
                    scriptContainerRef.current.removeChild(script);
                }
            };
        }
    }, []);

    return (
        <Layout
            title={siteConfig.tagline}
            description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
        >
            <header className={"hero"}>
                <div className="container">
                    <div className="hero__content">
                        <div>
                            <h1>Join the DataHub Slack Community!</h1>
                            <div style={{ fontSize: "18px" }}>We will send the link to join our Slack community to your email.</div>
                        </div>
                        <div style={{ width: "50%", margin: "3rem auto" }} ref={scriptContainerRef}>
                        </div>
                    </div>
                </div>
            </header>
        </Layout>
    );
}

export default SlackSurvey;
