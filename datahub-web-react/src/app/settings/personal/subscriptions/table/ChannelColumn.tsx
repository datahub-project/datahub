import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { SlackCircleFilled } from '@ant-design/icons';
import { DataHubSubscription, NotificationSinkType } from '../../../../../types.generated';
import { getSubscriptionChannel } from '../../../../shared/subscribe/drawer/utils';

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

type Props = {
    isPersonal: boolean;
    subscription: DataHubSubscription;
    settingsChannel?: string;
};

const SlackColumn = ({ isPersonal, subscription, settingsChannel }: Props) => {
    const subscriptionChannel = getSubscriptionChannel(isPersonal, subscription);
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

const sinkMap = {
    [NotificationSinkType.Slack]: SlackColumn,
} as const;

const ChannelColumn = (props: Props) => {
    const sinkTypes = Object.keys(sinkMap);
    return (
        <ChannelsContainer>
            {sinkTypes.map((sinkType) => {
                const Component = sinkMap[sinkType];
                return <Component key={sinkType} {...props} />;
            })}
        </ChannelsContainer>
    );
};

export default ChannelColumn;
