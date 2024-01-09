import React, { useEffect, useRef } from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function SlackSurvey() {
    const context = useDocusaurusContext();
    const { siteConfig = {} } = context;
    const scriptContainerRef = useRef(null); // Create a ref for the script container

    useEffect(() => {
        const script = document.createElement('script');
        script.src = '//js.hsforms.net/forms/embed/v2.js';
        script.async = true;
        script.charset = 'utf-8';
        script.type = 'text/javascript';
        script.onload = () => {
            if (window.hbspt) {
                window.hbspt.forms.create({
                    region: 'na1',
                    portalId: '14552909',
                    formId: '835d0abb-ab93-447b-9fb7-3a22702e9ebb',
                });
            }
        };

        scriptContainerRef.current.appendChild(script);

        return () => {
            scriptContainerRef.current.removeChild(script);
        };
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
                            <h1>Join Our Slack Community!</h1>
                        </div> 
                        <div>
                            <div style={{ width:"50%", margin:"3rem auto" }}ref={scriptContainerRef}>
                        </div> 
                        </div>
                    </div>
                </div>
            </header>
        </Layout>
    );
}

export default SlackSurvey;
