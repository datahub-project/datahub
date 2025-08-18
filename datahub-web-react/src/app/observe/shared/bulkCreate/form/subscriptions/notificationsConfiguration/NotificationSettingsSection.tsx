import { Text, colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { EmailNotificationSettings } from '@app/observe/shared/bulkCreate/form/subscriptions/notificationsConfiguration/EmailNotificationSettings';
import { SlackNotificationSettings } from '@app/observe/shared/bulkCreate/form/subscriptions/notificationsConfiguration/SlackNotificationSettings';
import useActorSinkSettings from '@app/shared/subscribe/drawer/useSinkSettings';

import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';

const SectionContainer = styled.div`
    margin-top: 16px;
    padding: 16px;
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    background-color: ${colors.gray[50]};
    box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
`;

const Subtitle = styled(Text)`
    margin-bottom: 16px;
`;

interface Props {
    isPersonal: boolean;
    groupUrn?: string;
}

export const NotificationSettingsSection: React.FC<Props> = ({ isPersonal, groupUrn }) => {
    const { data: globalSettings } = useGetGlobalSettingsQuery();

    const { emailSettings, slackSettings, updateSinkSettings, sinkTypes, loading, error } = useActorSinkSettings({
        isPersonal,
        groupUrn,
    });

    if (loading) {
        return <Text>Loading notification settings...</Text>;
    }

    if (error) {
        return (
            <Text size="md" color="gray" colorLevel={500} weight="normal">
                You can manage Notification delivery in the{' '}
                <Link
                    to={isPersonal ? `/settings/personal-notifications` : `/group/${groupUrn}/notifications`}
                    target="_blank"
                >
                    {isPersonal ? 'Personal' : 'Group'} notification settings page.
                </Link>
            </Text>
        );
    }

    return (
        <SectionContainer>
            <Text size="md" color="gray" colorLevel={600} weight="semiBold">
                Notification Delivery
            </Text>
            <Subtitle size="md" color="gray" colorLevel={500} weight="normal">
                These are global settings.{' '}
                <Link
                    to={isPersonal ? `/settings/personal-notifications` : `/group/${groupUrn}/notifications`}
                    target="_blank"
                >
                    Manage here
                </Link>
            </Subtitle>

            <EmailNotificationSettings
                isPersonal={isPersonal}
                emailSettings={emailSettings}
                sinkTypes={sinkTypes}
                globalSettings={globalSettings}
                onUpdateSinkSettings={updateSinkSettings}
            />

            <SlackNotificationSettings
                isPersonal={isPersonal}
                slackSettings={slackSettings}
                sinkTypes={sinkTypes}
                globalSettings={globalSettings}
                onUpdateSinkSettings={updateSinkSettings}
            />
        </SectionContainer>
    );
};
