/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import TimelineSkeleton from '@app/entityV2/shared/TimelineSkeleton';
import IncidentActivityAvatar from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentActivityAvatar';
import IncidentActivityContent from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentActivityContent';
import {
    ActivityLabelSection,
    ActivitySection,
    TimelineWrapper,
} from '@app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';
import { TimelineContentDetails } from '@app/entityV2/shared/tabs/Incident/types';
import { Timeline } from '@src/alchemy-components';

type IncidentActivitySectionProps = {
    loading: boolean;
    renderActivities: any[];
};

export const IncidentActivitySection = ({ loading, renderActivities }: IncidentActivitySectionProps) => {
    return (
        <ActivitySection>
            <ActivityLabelSection>Activity</ActivityLabelSection>
            {loading ? (
                <TimelineSkeleton />
            ) : (
                <TimelineWrapper>
                    <Timeline
                        items={renderActivities}
                        renderDot={(item) => <IncidentActivityAvatar user={item?.actor} />}
                        renderContent={(item: TimelineContentDetails) => (
                            <IncidentActivityContent incidentActivities={item} />
                        )}
                    />
                </TimelineWrapper>
            )}
        </ActivitySection>
    );
};
