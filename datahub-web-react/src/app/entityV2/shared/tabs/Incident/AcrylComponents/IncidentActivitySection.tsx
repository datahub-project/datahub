import React from 'react';
import { Timeline } from '@src/alchemy-components';

import IncidentActivityContent from './IncidentActivityContent';
import { ActivityLabelSection, ActivitySection, TimelineWrapper } from './styledComponents';
import { TimelineContentDetails } from '../types';
import IncidentActivityAvatar from './IncidentActivityAvatar';
import TimelineSkeleton from '../../../TimelineSkeleton';

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
