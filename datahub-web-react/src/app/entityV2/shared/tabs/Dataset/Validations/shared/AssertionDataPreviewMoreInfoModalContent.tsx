import { Text } from '@components';
import React from 'react';

import analytics, { EventType } from '@src/app/analytics';

const VIEW_DOCUMENTATION_LINK = 'https://datahubproject.io/integrations/';

const AssertionDataPreviewMoreInfoModalContent = () => {
    function sendAnalytics() {
        analytics.event({
            type: EventType.ClickViewDocumentation,
            location: 'statsTab',
            link: VIEW_DOCUMENTATION_LINK,
        });
    }

    return (
        <div>
            <Text color="gray" size="md">
                Historical stats are available for data sources that have profiling enabled with row counts collected
                within the last 30 days.
            </Text>
            <Text color="gray" size="md">
                You can{' '}
                <a href={VIEW_DOCUMENTATION_LINK} onClick={sendAnalytics} target="_blank" rel="noreferrer noopener">
                    view documentation
                </a>{' '}
                for more help.
            </Text>
        </div>
    );
};

export default AssertionDataPreviewMoreInfoModalContent;
