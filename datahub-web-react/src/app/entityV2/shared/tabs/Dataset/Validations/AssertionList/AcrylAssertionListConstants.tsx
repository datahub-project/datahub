import React from 'react';
import { Clock, Database, GitFork, Hammer, Dresser } from '@phosphor-icons/react';
import styled from 'styled-components';
import { CheckOutlined, CloseOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { AssertionResultType, AssertionType } from '@src/types.generated';

const StyledCardTitle = styled.div<{ background: string; color: string }>`
    background: ${({ background }) => background};
    color: ${({ color }) => color};
    padding: 4px;
    font-weight: 700;
    padding-left: 24px;
    gap: 8px;
    display: flex;
    align-items: center;
    font-size: 12px;
`;

export const ASSERTION_STATUS_WITH_COLOR_MAP = {
    passing: {
        color: '#548239',
        backgroundColor: '#F1F8EE',
        resultType: AssertionResultType.Success,
        icon: <CheckOutlined />,
        text: 'Passing',
        headerComponent: (
            <StyledCardTitle background="#F1F8EE" color={'#548239'}>
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
            <StyledCardTitle background="#FCF2F2" color={'#D23939'}>
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
            <StyledCardTitle background="#FEF9ED" color={'#EEAE09'}>
                <InfoCircleOutlined /> Error
            </StyledCardTitle>
        ),
    },
};

export const ASSERTION_STYPE_AND_ICON_MAP: Record<AssertionType, JSX.Element> = {
    [AssertionType.Freshness]: <Clock size={24} />,
    [AssertionType.Volume]: <Database size={24} />,
    [AssertionType.Field]: <Dresser size={24} />,
    [AssertionType.DataSchema]: <GitFork size={24} />,
    [AssertionType.Custom]: <Hammer size={24} />,
    [AssertionType.Sql]: <Database size={24} />,
    [AssertionType.Dataset]: <Database size={24} />,
};
