import React from 'react';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from "./events.module.scss";

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
                        <div style={{ fontSize: "18px" }}>Subscribe to join our monthly events to network and learn more about our community!</div>
                        <div className="lumaCalendar">
                            <iframe
                                src="https://lu.ma/embed/calendar/cal-lom9HnTVnZkKsNh/events"
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
