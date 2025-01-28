import React from 'react';
import styled from 'styled-components';
import { CheckOutlined, CloseOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { AssertionResultType } from '@src/types.generated';
import { NO_RUNNING_STATE } from './constant';

const StyledCardTitle = styled.div<{ background: string; color: string }>`
    background: ${({ background }) => background};
    color: ${({ color }) => color};
    padding: 8px;
    font-weight: 700;
    padding-left: 24px;
    gap: 8px;
    display: flex;
    align-items: center;
    font-size: 12px;
`;

export const ASSERTION_SUMMARY_CARD_HEADER_BY_STATUS = {
    passing: {
        color: '#548239',
        backgroundColor: '#F1F8EE',
        resultType: AssertionResultType.Success,
        icon: <CheckOutlined />,
        text: 'Passing',
        headerComponent: (
            <StyledCardTitle background="#F1F8EE" color="#548239">
                <CheckOutlined /> Passing
            </StyledCardTitle>
        ),
    },
    failing: {
        color: '#D23939',
        backgroundColor: '#FCF2F2',
        resultType: AssertionResultType.Failure,
        icon: <CloseOutlined />,
        text: 'Failing',
        headerComponent: (
            <StyledCardTitle background="#FCF2F2" color="#D23939">
                <CloseOutlined /> Failing
            </StyledCardTitle>
        ),
    },
    erroring: {
        color: '#EEAE09',
        backgroundColor: '#FEF9ED',
        resultType: AssertionResultType.Error,
        icon: <InfoCircleOutlined />,
        text: 'Errors',
        headerComponent: (
            <StyledCardTitle background="#FEF9ED" color="#EEAE09">
                <InfoCircleOutlined /> Error
            </StyledCardTitle>
        ),
    },
    [NO_RUNNING_STATE]: {
        color: '#8D95B1',
        backgroundColor: '#e0e0e0',
        resultType: null,
        icon: <InfoCircleOutlined />,
        text: '0 Running',
        headerComponent: (
            <StyledCardTitle background="#F6F6F6" color="#8D95B1">
                <InfoCircleOutlined /> No runs
            </StyledCardTitle>
        ),
    },
};
