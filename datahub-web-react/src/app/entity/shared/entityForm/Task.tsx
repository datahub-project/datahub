import { Test } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import { Icon, Text } from '@components';
import Loading from '@src/app/shared/Loading';

const TaskWrapper = styled.div`
    display: flex;
    align-items: center;
    padding: 8px 0;
`;

const TextWrapper = styled.div`
    margin-left: 8px;
    margin-right: auto;
`;

const TaskName = styled.span`
    font-size: 14px;
    font-weight: 700;
    margin-right: 6px;
`;

const IconWrapper = styled.div``;

interface Props {
    task: Test;
    isComplete: boolean;
}

export default function Task({ task, isComplete }: Props) {
    return (
        <TaskWrapper>
            <IconWrapper>
                {isComplete ? <Icon icon="CheckCircle" color="green" size="2xl" /> : <Loading height={16} />}
            </IconWrapper>
            <TextWrapper>
                <TaskName>{task.name || task.urn}</TaskName>
                {isComplete && <>Complete: {task.results.passingCount}</>}
            </TextWrapper>
            <Text color={isComplete ? 'green' : 'blue'}>{isComplete ? 'Complete' : 'Active'}</Text>
        </TaskWrapper>
    );
}
