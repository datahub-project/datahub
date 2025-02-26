import React from 'react';
import styled from 'styled-components';

import { InfoCircleFilled } from '@ant-design/icons';
import { colors, PageTitle } from '@src/alchemy-components';

import { PlatformNotificationsScenarioSettings } from './PlatformNotificationsScenarioSettings';
import { DefaultsCard } from './DefaultsCard';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';

const Container = styled.div`
    width: 100%;
    overflow: auto;
    padding: 16px 20px;
`;

const GlobalNotificationsBanner = styled.div`
    background: #f9f0ff;
    border-radius: 8px;
    border: 1px solid ${colors.violet['500']};
    padding: 8px 12px;
    margin: 18px 0 25px;
    font-size: 14px;
`;

const InfoIcon = styled(InfoCircleFilled)`
    color: ${colors.violet['500']};
    margin-right: 8px;
`;

export const PlatformNotifications = () => {
    // Fetch global notification settings. Ideally we should use a cache here to avoid a refetch from this specific component.
    const { data, loading, error, refetch } = useGetGlobalSettingsQuery();
    return (
        <Container>
            <PageTitle title="Notifications" subTitle="Customize when and where you receive platform notifications" />
            <GlobalNotificationsBanner>
                <InfoIcon />
                These notifications settings are global. To set up notifications for a specific entity, you can{' '}
                <a
                    href="https://datahubproject.io/docs/next/managed-datahub/subscription-and-notification/"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    subscribe
                </a>{' '}
                to that entity.
            </GlobalNotificationsBanner>
            <DefaultsCard refetch={refetch} globalSettings={data?.globalSettings || undefined} />
            <PlatformNotificationsScenarioSettings
                loading={loading}
                error={error}
                refetch={refetch}
                globalSettings={data?.globalSettings || undefined}
            />
        </Container>
    );
};
