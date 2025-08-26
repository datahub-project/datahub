import { blue } from '@ant-design/colors';
import { CheckCircleFilled } from '@ant-design/icons';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import {
    BULK_VERIFY_ID,
    getAssociatedPromptId,
    getFirstTestResult,
} from '@app/entity/shared/entityForm/useEntityFormTasks';
import Loading from '@src/app/shared/Loading';
import { Test } from '@src/types.generated';

const TaskWrapper = styled.div`
    padding: 12px 0;
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

const StyledCheck = styled(CheckCircleFilled)`
    color: green;
    font-size: 16px;
`;

const SubmittedText = styled.span<{ $color: string }>`
    color: ${(props) => props.$color};
`;

const TaskHeader = styled.div`
    display: flex;
    align-items: center;
`;

const IconWrapper = styled.div`
    height: 16px;
    width: 16px;
`;

const SubHeader = styled.div`
    margin-left: 25px;
    color: gray;
`;

const Reload = styled.span`
    color: ${blue[6]};
    cursor: pointer;
`;

interface Props {
    task: Test;
    isComplete: boolean;
    allTasks: Test[];
}

export default function Task({ task, isComplete, allTasks }: Props) {
    const {
        setShouldRefetch,
        refetchForBulk,
        prompt: { selectedPromptId },
        form: { formView },
    } = useEntityFormContext();
    const associatedPromptId = useMemo(() => getAssociatedPromptId(task.urn), [task.urn]);

    function handleReload() {
        if (
            (formView === FormView.BULK_VERIFY && associatedPromptId === BULK_VERIFY_ID) ||
            selectedPromptId === associatedPromptId
        ) {
            refetchForBulk();
            setShouldRefetch(true);
        }
    }

    // if multiple tasks have the same name, add a (1) etc to the end.
    const tasksWithSameName = allTasks.filter((t) => t.name === task.name);
    const myNameIndex = tasksWithSameName.reverse().findIndex((t) => t.urn === task.urn);
    const firstResult = getFirstTestResult(task);

    return (
        <TaskWrapper>
            <TaskHeader>
                <IconWrapper>{isComplete ? <StyledCheck /> : <Loading height={16} marginTop={0} />}</IconWrapper>
                <TextWrapper>
                    <TaskName>
                        {task.name || task.urn} {myNameIndex > 0 && `(${myNameIndex})`}
                    </TaskName>
                </TextWrapper>
                <SubmittedText $color={isComplete ? 'green' : blue[6]}>
                    {isComplete ? `${firstResult?.result?.passingCount || 0} submitted` : 'Submitting...'}
                </SubmittedText>
            </TaskHeader>
            {isComplete && (
                <SubHeader>
                    Processing a large number of responses may take some time after submitting.{' '}
                    <Reload onClick={handleReload}>Reload</Reload>
                </SubHeader>
            )}
        </TaskWrapper>
    );
}
