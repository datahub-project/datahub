import React from 'react';
import styled from 'styled-components';
import { Tooltip, Typography } from 'antd';
import { getAssertionGroupSummaryIcon } from './acrylUtils';
import { AssertionGroup } from './acrylTypes';

const Container = styled.div`
    display: flex;
    align-items: center;
    &:hover {
        cursor: pointer;
    }
`;

const TextContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    font-size: 14px;
`;

const Title = styled(Typography.Text)`
    && {
        padding-bottom: 0px;
        margin-bottom: 0px;
    }
`;

const Message = styled(Typography.Text)`
    && {
        font-size: 12px;
        margin-left: 8px;
    }
`;

const SummaryIcon = styled.div`
    margin-right: 16px;
`;

type Props = {
    group: AssertionGroup;
};

export const AssertionGroupHeader = ({ group }: Props) => {
    const { summary } = group;
    const summaryIcon = getAssertionGroupSummaryIcon(summary);
    const summaryMessage = `${summary.succeededRuns} passing assertions, ${summary.failedRuns} failing assertions`;
    return (
        <Tooltip title="This status is based on the most recent run of each assertion.">
            <Container>
                {summaryIcon && <SummaryIcon>{summaryIcon}</SummaryIcon>}
                <TextContainer>
                    {group.icon}
                    <Title strong>{group.name}</Title>
                    <Message type="secondary">{summaryMessage}</Message>
                </TextContainer>
            </Container>
        </Tooltip>
    );
};
