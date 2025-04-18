import { CheckCircleFilled, CloseCircleFilled } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX } from '../../../../entity/shared/tabs/Incident/incidentUtils';
import { ACTION_TYPES } from '../definition/builder/property/types/action';
import { deserializeTestDefinition, serializeTestDefinition } from '../definition/utils';
import { ActionsBuilder } from '../definition/builder/action/ActionsBuilder';
import { Action } from './types';
import { TestBuilderState } from '../../types';

const ActionSection = styled.div`
    margin-bottom: 20px;
`;

const ActionSectionTitle = styled.div`
    margin-top: 8px;
    margin-bottom: 12px;
    padding-left: 4px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

const ActionsContainer = styled.div`
    padding: 8px;
`;

const FailureIcon = styled(CloseCircleFilled)`
    color: ${FAILURE_COLOR_HEX};
    font-size: 18px;
`;

const SuccessIcon = styled(CheckCircleFilled)`
    color: ${SUCCESS_COLOR_HEX};
    font-size: 20px;
`;

const StatusTitle = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
        margin-left: 8px;
        font-size: 12px;
    }
`;

const ActionSelect = styled.div`
    margin-bottom: 8px;
    margin-top: 12px;
    margin-right: 12px;
`;

const SubTitle = styled(Typography.Paragraph)`
    font-size: 14px;
`;

type Props = {
    state: TestBuilderState;
    updateState: (newState: TestBuilderState) => void;
};

export const ActionsStep = ({ state, updateState }: Props) => {
    const testDefinition = useMemo(() => deserializeTestDefinition(state?.definition?.json || '{}'), [state]);

    const selectedPassingActions = testDefinition.actions?.passing || [];
    const selectedFailingActions = testDefinition.actions?.failing || [];

    const onSetPassingActions = (newActions: Action[]) => {
        const newDefinition = {
            ...testDefinition,
            actions: {
                passing: newActions,
                failing: testDefinition.actions?.failing || [],
            },
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    const onSetFailingActions = (newActions: Action[]) => {
        const newDefinition = {
            ...testDefinition,
            actions: {
                passing: testDefinition.actions?.passing || [],
                failing: newActions,
            },
        };
        const newState = {
            ...state,
            definition: {
                json: serializeTestDefinition(newDefinition),
            },
        };
        updateState(newState);
    };

    return (
        <>
            <Typography.Title level={5}>Add custom actions</Typography.Title>
            <SubTitle type="secondary">
                What actions would you like to apply to the data assets that pass or fail the conditions?
            </SubTitle>
            <ActionsContainer>
                <ActionSection>
                    <ActionSectionTitle>
                        <SuccessIcon />
                        <StatusTitle>Passing Assets</StatusTitle>
                    </ActionSectionTitle>
                    <ActionsBuilder
                        actionTypes={ACTION_TYPES}
                        selectedActions={selectedPassingActions}
                        onChangeActions={onSetPassingActions}
                    />
                </ActionSection>
                <ActionSection>
                    <ActionSectionTitle>
                        <FailureIcon />
                        <StatusTitle>Failing Assets</StatusTitle>
                    </ActionSectionTitle>
                    <ActionSelect>
                        <ActionsBuilder
                            actionTypes={ACTION_TYPES}
                            selectedActions={selectedFailingActions}
                            onChangeActions={onSetFailingActions}
                        />
                    </ActionSelect>
                </ActionSection>
            </ActionsContainer>
        </>
    );
};
