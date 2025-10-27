import { MailOutlined, SlackOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    getEmailSettingsChannel,
    getEmailSubscriptionChannel,
    getSinkTypesForSubscription,
    getSlackSettingsChannel,
    getSlackSubscriptionChannel,
    getTeamsSettingsChannel,
    getTeamsSubscriptionChannel,
} from '@app/shared/subscribe/drawer/utils';

import {
    DataHubSubscription,
    EmailNotificationSettings,
    NotificationSinkType,
    SlackNotificationSettings,
    TeamsNotificationSettings,
} from '@types';

import teamsLogo from '@images/teamslogo.png';

const ChannelsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SlackChannelContainer = styled.div`
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

const EmailContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const EmailIcon = styled(MailOutlined)`
    font-size: 16px;
`;

const TeamsChannelContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const TeamsIcon = styled.img`
    font-size: 16px;
    height: 16px;
    width: 16px;
`;

type Props = {
    isPersonal: boolean;
    subscription: DataHubSubscription;
    emailSettings?: EmailNotificationSettings;
    slackSettings?: SlackNotificationSettings;
    teamsSettings?: TeamsNotificationSettings;
};

const SlackColumn = ({
    isPersonal,
    subscription,
    slackSettings,
}: Pick<Props, 'isPersonal' | 'subscription' | 'slackSettings' | 'teamsSettings'>) => {
    const settingsChannel = getSlackSettingsChannel(isPersonal, slackSettings);
    const subscriptionChannel = getSlackSubscriptionChannel(isPersonal, subscription);
    const useDefault = !!settingsChannel && !subscriptionChannel;
    const channel = useDefault ? settingsChannel : subscriptionChannel;

    if (!channel) return null;

    return (
        <SlackChannelContainer>
            <SlackIcon />
            <Typography.Text>
                {channel} {useDefault && <Typography.Text type="secondary">(default)</Typography.Text>}
            </Typography.Text>
        </SlackChannelContainer>
    );
};

const TeamsColumn = ({
    isPersonal,
    subscription,
    teamsSettings,
}: Pick<Props, 'isPersonal' | 'subscription' | 'teamsSettings'>) => {
    const settingsChannel = getTeamsSettingsChannel(isPersonal, teamsSettings);
    const subscriptionChannel = getTeamsSubscriptionChannel(isPersonal, subscription);
    const useDefault = !!settingsChannel && !subscriptionChannel;
    const channel = useDefault ? settingsChannel : subscriptionChannel;

    if (!channel) return null;

    return (
        <TeamsChannelContainer>
            <TeamsIcon src={teamsLogo} alt="Teams" />
            <Typography.Text>
                {channel} {useDefault && <Typography.Text type="secondary">(default)</Typography.Text>}
            </Typography.Text>
        </TeamsChannelContainer>
    );
};

const EmailColumn = ({
    isPersonal,
    subscription,
    emailSettings,
}: Pick<Props, 'isPersonal' | 'subscription' | 'emailSettings'>) => {
    const settingsEmail = getEmailSettingsChannel(isPersonal, emailSettings);
    const subscriptionEmail = getEmailSubscriptionChannel(isPersonal, subscription);
    const useDefault = !!settingsEmail && !subscriptionEmail;
    const finalEmail = useDefault ? settingsEmail : subscriptionEmail;

    if (!finalEmail) return null;

    return (
        <EmailContainer>
            <EmailIcon />
            <Typography.Text>
                {finalEmail} {useDefault && <Typography.Text type="secondary">(default)</Typography.Text>}
            </Typography.Text>
        </EmailContainer>
    );
};

const sinkMap = {
    [NotificationSinkType.Slack]: SlackColumn,
    [NotificationSinkType.Email]: EmailColumn,
    [NotificationSinkType.Teams]: TeamsColumn,
} as const;

const ChannelColumn = (props: Props) => {
    const sinkTypesForSubscription = getSinkTypesForSubscription(props.subscription);
    const sinkTypesToShow = Object.keys(sinkMap).filter((sinkType) =>
        sinkTypesForSubscription.includes(sinkType as NotificationSinkType),
    ) as NotificationSinkType[];
    return (
        <ChannelsContainer>
            {sinkTypesToShow.map((sinkType) => {
                const Component = sinkMap[sinkType];
                return <Component key={sinkType} {...props} />;
            })}
        </ChannelsContainer>
    );
};

export default ChannelColumn;
