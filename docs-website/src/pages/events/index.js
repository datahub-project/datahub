import React from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Events() {
    const { siteConfig = {} } = useDocusaurusContext();

    return (
        <Layout
            title={siteConfig.tagline}
            description="Description of the page">
            <header className={"hero"}>
                <div className="container">
                    <div className="hero__content">
                        <h1>DataHub Community Event Calendar</h1>
                        <div style={{ fontSize: "18px" }}>Join our monthly events to network and learn more about our community!</div>
                        <div className="lumaCalendar" style={{ maxWidth: "60rem", margin: "3rem auto" }}>
                            <iframe
                                src="https://lu.ma/embed/calendar/cal-lom9HnTVnZkKsNh/events"
                                allowFullScreen={true}
                                aria-hidden="false"
                                tabIndex="0"
                                style={{
                                    border:"1px solid #bfcbda88",
                                    borderRadius:"10px",
                                    width:"100%",
                                    height:"600px",
                                }}
                            ></iframe>
                        </div> 
                    </div>
                </div>
            </header>
        </Layout>
    );
}

export default Events;
