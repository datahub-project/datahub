import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { AssertionGroup } from '../acrylTypes';

const Container = styled.div`
    display: flex;
    align-items: center;
    padding: 4px 0px;
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

type Props = {
    group: AssertionGroup;
};

export const AssertionGroupHeader = ({ group }: Props) => {
    const { summary } = group;
    const inactiveCount = summary.totalAssertions - summary.total;
    const summaryMessage = `${summary.passing} passing, ${summary.failing} failing${
        summary.erroring ? `, ${summary.erroring} errors` : ''
    }${inactiveCount ? `, ${inactiveCount} inactive` : ''}`;
    return (
        <Container>
            <TextContainer>
                <Title strong>{group.name}</Title>
                <Message type="secondary">{summaryMessage}</Message>
            </TextContainer>
        </Container>
    );
};
