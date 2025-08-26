import { MailOutlined, SlackCircleFilled } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    getEmailSettingsChannel,
    getEmailSubscriptionChannel,
    getSinkTypesForSubscription,
    getSlackSettingsChannel,
    getSlackSubscriptionChannel,
} from '@app/shared/subscribe/drawer/utils';

import {
    DataHubSubscription,
    EmailNotificationSettings,
    NotificationSinkType,
    SlackNotificationSettings,
} from '@types';

const ChannelsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SlackChannelContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const SlackIcon = styled(SlackCircleFilled)`
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

type Props = {
    isPersonal: boolean;
    subscription: DataHubSubscription;
    emailSettings?: EmailNotificationSettings;
    slackSettings?: SlackNotificationSettings;
};

const SlackColumn = ({
    isPersonal,
    subscription,
    slackSettings,
}: Pick<Props, 'isPersonal' | 'subscription' | 'slackSettings'>) => {
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
