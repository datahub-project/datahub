import { Text } from '@components';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getPlatformName } from '@src/app/entityV2/shared/utils';
import React from 'react';
import analytics, { EventType } from '@src/app/analytics';
import { useStatsSectionsContext } from '../../StatsSectionsContext';

const VIEW_DOCUMENTATION_LINK = 'https://datahubproject.io/integrations/';

const MoreInfoModalContent = () => {
    const { statsEntity } = useStatsSectionsContext();
    const platformName = getPlatformName(statsEntity as GenericEntityProperties);

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
                Turn on profiling for the {platformName} source in order to see stats here.
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

export default MoreInfoModalContent;
