import React from 'react';
import { Link } from 'react-router-dom';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { colors, Text } from '@src/alchemy-components';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { ActivityStatusText, Content, ContentRow } from './styledComponents';
import { TimelineContentDetails } from '../types';
import useGetUserName from '../hooks';

type TimelineContentProps = {
    incidentActivities: TimelineContentDetails;
};

export default function IncidentActivityContent({ incidentActivities }: TimelineContentProps) {
    const { action, actor, time } = incidentActivities;
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
                    <Text color="gray" type="span" style={{ color: colors.gray[1700] }}>
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
                <Text style={{ color: colors.gray[1700] }}>{getTimeFromNow(time)}</Text>
            </ContentRow>
        </Content>
    );
}
