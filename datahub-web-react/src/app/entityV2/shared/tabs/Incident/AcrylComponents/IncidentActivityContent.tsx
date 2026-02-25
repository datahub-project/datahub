import React from 'react';
import { Link } from 'react-router-dom';
import { useTheme } from 'styled-components';

import {
    ActivityStatusText,
    Content,
    ContentRow,
} from '@app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';
import useGetUserName from '@app/entityV2/shared/tabs/Incident/hooks';
import { TimelineContentDetails } from '@app/entityV2/shared/tabs/Incident/types';
import { Text } from '@src/alchemy-components';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

type TimelineContentProps = {
    incidentActivities: TimelineContentDetails;
};

export default function IncidentActivityContent({ incidentActivities }: TimelineContentProps) {
    const theme = useTheme();
    const { action, actor, time, message } = incidentActivities;
    const getUserName = useGetUserName();
    const entityRegistry = useEntityRegistryV2();

    return (
        <Content>
            <ContentRow>
                <Text
                    style={{
                        display: 'flex',
                        flexDirection: 'row',
                        gap: '4px',
                    }}
                >
                    <ActivityStatusText>{action}</ActivityStatusText>
                    <Text type="span" style={{ color: theme.colors.textSecondary }}>
                        by
                    </Text>
                    <ActivityStatusText>
                        {actor && (
                            <Link to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}>
                                {getUserName(actor)}
                            </Link>
                        )}
                    </ActivityStatusText>
                </Text>
                <Text style={{ color: theme.colors.textSecondary }}>{getTimeFromNow(time)}</Text>
                {message ? <Text style={{ color: theme.colors.textSecondary }}>{message}</Text> : null}
            </ContentRow>
        </Content>
    );
}
