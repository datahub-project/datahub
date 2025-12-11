/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Text } from '@components';
import React from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import analytics, { EventType } from '@src/app/analytics';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getPlatformNameFromEntityData } from '@src/app/entityV2/shared/utils';

const VIEW_DOCUMENTATION_LINK = 'https://datahubproject.io/integrations/';

const MoreInfoModalContent = () => {
    const { statsEntity } = useStatsSectionsContext();
    const platformName = getPlatformNameFromEntityData(statsEntity as GenericEntityProperties);

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
