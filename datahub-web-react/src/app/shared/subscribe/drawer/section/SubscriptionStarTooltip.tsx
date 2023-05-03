import React from 'react';
import styled from 'styled-components/macro';
import { Divider, Typography } from 'antd';
import { TeamOutlined } from '@ant-design/icons';

const TextContainer = styled.div`
    border-radius: 2px;
    min-width: 200px;
`;

const HeadingText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 19px;
    font-weight: 700;
    color: #ffffff;
`;

const GroupSectionContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const GroupListContainer = styled.div`
    margin-left: 10px;
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const GroupContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 4px;
    align-items: center;
`;

interface Props {
    isSubscribed: boolean;
    numUserSubscriptions: number;
    numGroupSubscriptions: number;
    groupNames: string[];
}

export default function SubscriptionStarTooltip({
    isSubscribed,
    numUserSubscriptions,
    numGroupSubscriptions,
    groupNames,
}: Props) {
    const userText = numUserSubscriptions === 1 ? 'user' : 'users';
    const isUserSubscribedText = isSubscribed ? '' : 'not ';
    const userSubscriptionText = `${numUserSubscriptions} ${userText} subscribed - ${isUserSubscribedText} including you`;
    const groupText = numGroupSubscriptions === 1 ? 'group is' : 'groups are';
    const groupSubscriptionText = `${numGroupSubscriptions} ${groupText} subscribed`;

    return (
        <TextContainer>
            <HeadingText>{userSubscriptionText}</HeadingText>
            <Divider style={{ backgroundColor: 'white' }} />
            <GroupSectionContainer>
                <HeadingText>{groupSubscriptionText}</HeadingText>
                <GroupListContainer>
                    {groupNames.map((groupName) => (
                        <GroupContainer>
                            <TeamOutlined />
                            <HeadingText key={groupName}>{groupName}</HeadingText>
                        </GroupContainer>
                    ))}
                </GroupListContainer>
            </GroupSectionContainer>
        </TextContainer>
    );
}
