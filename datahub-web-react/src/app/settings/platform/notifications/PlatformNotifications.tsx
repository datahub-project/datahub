import React from 'react';
import styled from 'styled-components';

import { InfoCircleFilled } from '@ant-design/icons';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { PlatformNotificationsConfigurationCard } from './PlatformNotificationsConfigurationCard';
import { DefaultsCard } from './DefaultsCard';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';

const Container = styled.div`
    width: 100%;
    overflow: auto;
    padding-top: 40px;
    padding-right: 40px;
    padding-left: 40px;
    padding-bottom: 40px;
`;

const SubHeader = styled(Typography.Text)`
    color: ${ANTD_GRAY[9]};
    font-size: 14px;
`;

const GlobalNotificationsBanner = styled.div`
    background: #f9f0ff;
    border-radius: 8px;
    border: 1px solid #984ecc;
    padding: 8px 16px;
    margin: 18px 0 25px;
    font-size: 14px;
`;

const InfoIcon = styled(InfoCircleFilled)`
    color: #7532a4;
    margin-right: 8px;
`;

export const PlatformNotifications = () => {
    // Fetch global notification settings. Ideally we should use a cache here to avoid a refetch from this specific component.
    const { data, loading, error, refetch } = useGetGlobalSettingsQuery();
    return (
        <Container>
            <Typography.Title level={3}>Notifications</Typography.Title>
            <SubHeader type="secondary">Select when and where global notifications are sent.</SubHeader>
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
            <PlatformNotificationsConfigurationCard
                loading={loading}
                error={error}
                refetch={refetch}
                globalSettings={data?.globalSettings || undefined}
            />
        </Container>
    );
};
