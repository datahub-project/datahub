import React from 'react';
import styled from 'styled-components/macro';
import { Divider, Typography } from 'antd';
import { CheckCircleFilled, TeamOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const TextContainer = styled.div`
    border-radius: 2px;
    min-width: 200px;
    max-width: 240px;
    font-size: 14px;
`;

const HeadingText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    font-weight: 500;
    color: #ffffff;
    display: flex;
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

const StyledDivider = styled(Divider)`
    background-color: ${ANTD_GRAY[7]};
    margin: 6px 0;
`;

const StyledCheck = styled(CheckCircleFilled)`
    font-size: 12px;
    margin-right: 4px;
    line-height: 26px;
`;

const BoldText = styled.span`
    font-weight: 700;
`;

const MAX_DISPLAYED_GROUPS = 5;

interface Props {
    isUserSubscribed: boolean;
    numUserSubscriptions: number;
    numGroupSubscriptions: number;
    groupNames: string[];
}

export default function SubscriptionStarTooltip({
    isUserSubscribed,
    numUserSubscriptions,
    numGroupSubscriptions,
    groupNames,
}: Props) {
    const userText = numUserSubscriptions === 1 ? 'user' : 'users';
    const userSubscriptionText = isUserSubscribed
        ? `${userText} including you ${numUserSubscriptions === 1 ? 'is' : 'are'} subscribed`
        : `${userText} ${numUserSubscriptions === 1 ? 'is' : 'are'} subscribed ${
              numUserSubscriptions > 0 ? `- not including you` : ''
          }`;
    const groupText = numGroupSubscriptions === 1 ? 'group is' : 'groups are';
    const groupSubscriptionText = `${groupText} subscribed`;

    return (
        <TextContainer>
            <HeadingText>
                {isUserSubscribed && <StyledCheck />}
                <span>
                    <BoldText>{numUserSubscriptions}</BoldText> {userSubscriptionText}
                </span>
            </HeadingText>
            <StyledDivider />
            <GroupSectionContainer>
                <HeadingText>
                    <span>
                        <BoldText>{numGroupSubscriptions}</BoldText> {groupSubscriptionText}
                    </span>
                </HeadingText>
                <GroupListContainer>
                    {groupNames.map((groupName) => (
                        <GroupContainer key={groupName}>
                            <TeamOutlined />
                            <HeadingText>{groupName}</HeadingText>
                        </GroupContainer>
                    ))}
                </GroupListContainer>
                {numGroupSubscriptions > MAX_DISPLAYED_GROUPS && (
                    <span>
                        +{numGroupSubscriptions - MAX_DISPLAYED_GROUPS} other group
                        {numGroupSubscriptions - MAX_DISPLAYED_GROUPS === 1 ? '' : 's'}
                    </span>
                )}
            </GroupSectionContainer>
        </TextContainer>
    );
}
