import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('entity.profile.incident');
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
                    <Trans
                        i18nKey="activity.actionByActor"
                        t={t}
                        components={{
                            status: <ActivityStatusText />,
                            actor: (
                                <ActivityStatusText>
                                    {actor && (
                                        <Link to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}>
                                            {getUserName(actor)}
                                        </Link>
                                    )}
                                </ActivityStatusText>
                            ),
                        }}
                        values={{ action }}
                    />
                </Text>
                <Text style={{ color: theme.colors.textSecondary }}>{getTimeFromNow(time)}</Text>
                {message ? <Text style={{ color: theme.colors.textSecondary }}>{message}</Text> : null}
            </ContentRow>
        </Content>
    );
}
