import React, { useEffect } from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function SlackSurvey() {
    const { siteConfig = {} } = useDocusaurusContext();

    useEffect(() => {
        // Redirect to the Acryl Slack page
        window.location.href = 'https://pages.acryl.io/slack';
    }, []);

    // Return minimal layout in case redirect takes a moment
    return (
        <Layout
            title={siteConfig.tagline}
            description="Description of the page">
            <header className={"hero"}>
                <div className="container">
                    <div className="hero__content">
                        <h1>Redirecting to Slack signup...</h1>
                    </div>
                </div>
            </header>
        </Layout>
    );
}

export default SlackSurvey;
