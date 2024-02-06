import React from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Events() {
    const { siteConfig = {} } = useDocusaurusContext();

    const iframeStyle = {
        border: "1px solid #bfcbda88",
        borderRadius: "10px"
    };

    return (
        <Layout
            title={siteConfig.tagline}
            description="Description of the page">
            <header className={"hero"}>
                <div className="container">
                    <div className="hero__content">
                        <h1>DataHub Community Event Calendar</h1>
                        <div style={{ fontSize: "18px" }}>Subscribe to join our monthly events to network and learn more about our community!</div>
                        <div id="lumaCalendar" style={{ width: "60%", margin: "3rem auto" }}>
                            <iframe
                                src="https://lu.ma/embed/calendar/cal-lom9HnTVnZkKsNh/events"
                                width="100%"
                                height="600"
                                frameBorder="0"
                                style={iframeStyle}
                                allowFullScreen={true}
                                aria-hidden="false"
                                tabIndex="0"
                            ></iframe>
                        </div> 
                    </div>
                </div>
            </header>
        </Layout>
    );
}

export default Events;
