import { MailOutlined, SlackOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { getMergedNotificationSettingsForSubscription } from '@app/settingsV2/personal/subscriptions/utils';

import {
    CorpGroup,
    DataHubSubscription,
    EntityType,
    NotificationSettings,
    NotificationSinkType,
    TeamsChannel,
} from '@types';

import teamsLogo from '@images/teamslogo.png';

const ChannelsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const ChannelContainer = styled.div`
    display: inline-flex;
    align-items: center;
    width: fit-content;
    padding: 2px 6px 2px 4px;
    border-radius: 20px;
    border: 1px solid ${colors.gray[1400]};
    gap: 8px;
`;

const SlackIcon = styled(SlackOutlined)`
    font-size: 16px;
`;

const EmailIcon = styled(MailOutlined)`
    font-size: 16px;
`;

const TeamsIcon = styled.img`
    font-size: 16px;
    height: 16px;
    width: 16px;
`;

type Props = {
    subscription: DataHubSubscription;
    actorUrn: string;
    actorNotificationSettings?: NotificationSettings;
    ownedAndMemberGroup: CorpGroup[];
};

const SlackColumn = ({
    shouldUseHandle,
    userHandle,
    channels,
}: {
    shouldUseHandle: boolean;
    userHandle: string | undefined;
    channels: string[] | undefined;
}) => {
    if (shouldUseHandle) {
        if (!userHandle) return null;
        return (
            <ChannelContainer>
                <SlackIcon />
                <Typography.Text>{userHandle}</Typography.Text>
            </ChannelContainer>
        );
    }

    if (!channels?.length) return null;
    return (
        <>
            {channels.map((channel) => {
                return (
                    <ChannelContainer key={channel}>
                        <SlackIcon />
                        <Typography.Text>{channel}</Typography.Text>
                    </ChannelContainer>
                );
            })}
        </>
    );
};

const TeamsColumn = ({
    shouldUseDisplayName,
    teamsUserDisplayName,
    teamsChannels,
}: {
    shouldUseDisplayName: boolean;
    teamsUserDisplayName: string | undefined;
    teamsChannels: TeamsChannel[] | undefined;
}) => {
    if (shouldUseDisplayName) {
        if (!teamsUserDisplayName) return null;
        return (
            <ChannelContainer>
                <TeamsIcon src={teamsLogo} alt="Teams" />
                <Typography.Text>{teamsUserDisplayName}</Typography.Text>
            </ChannelContainer>
        );
    }
    if (!teamsChannels?.length) return null;
    return (
        <>
            {teamsChannels.map((channel) => {
                return (
                    <ChannelContainer key={channel.id}>
                        <TeamsIcon src={teamsLogo} alt="Teams" />
                        <Typography.Text>{channel.name || channel.id}</Typography.Text>
                    </ChannelContainer>
                );
            })}
        </>
    );
};

const EmailColumn = ({ email }: { email: string | undefined }) => {
    if (!email) return null;
    return (
        <ChannelContainer>
            <EmailIcon />
            <Typography.Text>{email}</Typography.Text>
        </ChannelContainer>
    );
};

const SubscriptionsChannelColumn = ({
    subscription,
    actorUrn,
    actorNotificationSettings,
    ownedAndMemberGroup,
}: Props) => {
    const mergedNotificationSettings = getMergedNotificationSettingsForSubscription(
        subscription,
        ownedAndMemberGroup,
        actorUrn,
        actorNotificationSettings,
    );
    const sinkTypesToShow = mergedNotificationSettings.sinkTypes || [];
    const subscriptionOwnerIsUser = subscription.actor.type === EntityType.CorpUser;
    return (
        <ChannelsContainer>
            {sinkTypesToShow.map((sinkType) => {
                switch (sinkType) {
                    case NotificationSinkType.Slack:
                        return (
                            <SlackColumn
                                key={sinkType}
                                shouldUseHandle={subscriptionOwnerIsUser}
                                userHandle={mergedNotificationSettings.slackUserHandle}
                                channels={mergedNotificationSettings.slackChannels}
                            />
                        );
                    case NotificationSinkType.Email:
                        return <EmailColumn key={sinkType} email={mergedNotificationSettings.email} />;
                    case NotificationSinkType.Teams:
                        return (
                            <TeamsColumn
                                key={sinkType}
                                shouldUseDisplayName={subscriptionOwnerIsUser}
                                teamsUserDisplayName={mergedNotificationSettings.teamsUserDisplayName}
                                teamsChannels={mergedNotificationSettings.teamsChannels}
                            />
                        );
                    default:
                        throw new Error(`Unhandled sink type: ${sinkType}`);
                }
            })}
        </ChannelsContainer>
    );
};

export default SubscriptionsChannelColumn;
