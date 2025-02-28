import React, { useEffect, useMemo } from 'react';

import { Delete, Add } from '@mui/icons-material';
import { Select } from 'antd';
import uniqid from 'uniqid';

import { PrimaryButton, TextButton, DeleteButton, SuccessIcon, FailureIcon } from '@app/automations/sharedComponents';
import { SelectInputMode } from '../types/values';

import {
    EmptyStateContainer,
    CustomActionsContainer,
    CustomActionBuilderContainer,
    BuilderGroupContainer,
    BuilderGroupHeader,
    ActionGroupContainer,
    ActionSelectorContainer,
    ActionsButtonContainer,
} from './components';

import { EntityTypeSelector } from '../EntityTypeSelector';

import { ACTION_TYPES } from './action';

type Action = {
    id: number;
    action: string;
    values: any[];
    updateAction?: any;
    updateValues?: any;
    removeAction?: any;
};

const actionOptions = ACTION_TYPES.map((action) => ({
    label: action.displayName,
    value: action.id,
}));

// Condition Selector
const ActionSelector = ({ ...props }: any) => {
    const selectedAction = ACTION_TYPES.find((action) => action.id === props.action);

    return (
        <ActionSelectorContainer>
            <div>
                <Select
                    options={actionOptions}
                    defaultValue={props.action || actionOptions[0].value}
                    showSearch={false}
                    onChange={(value: any) => props.updateAction(value, props.id)}
                />
            </div>
            {selectedAction?.valueOptions.mode === SelectInputMode.NONE && <div>{selectedAction?.description}</div>}
            {selectedAction?.valueOptions.mode !== SelectInputMode.NONE && (
                <div>
                    <EntityTypeSelector
                        state={props.values}
                        props={selectedAction?.valueOptions}
                        passStateToParent={(values: any) => props.updateValues(values, props.id)}
                    />
                </div>
            )}
            <div>
                <DeleteButton shape="circle" icon={<Delete />} onClick={() => props.removeAction(props.id)} />
            </div>
        </ActionSelectorContainer>
    );
};

const ActionBuilder = ({ selectedActions }: any) => {
    // Default Action
    const defaultAction: Action = {
        id: uniqid(),
        action: actionOptions[0].value,
        values: [],
    };

    // Util to format selected actions
    const formattedSelectedActions = useMemo(() => {
        return (
            selectedActions?.map((act: any) => ({
                id: uniqid(),
                action: act.type,
                values: act.values,
            })) || []
        );
    }, [selectedActions]);

    // State to hold the actions
    const [actions, setActions] = React.useState<Action[]>([]);

    useEffect(() => {
        if (formattedSelectedActions.length > 0 && actions.length === 0) {
            setActions(formattedSelectedActions);
        }
    }, [formattedSelectedActions, actions.length, setActions]);

    // Util to find the index of an action
    const findActionIndex = (list: any, id: number) => list.findIndex((c: any) => c.id === id);

    // Remove an action
    const removeAction = (id: number) => {
        const updatedConditions = [...actions];
        const index = findActionIndex(updatedConditions, id);
        if (index !== -1) {
            updatedConditions.splice(index, 1);
            setActions(updatedConditions);
        }
    };

    // Util to DRY update condition functions
    const helperUpdateAction = (id: any, field: any, value: any) => {
        const index = findActionIndex(actions, id);
        actions[index][field] = value;
        setActions([...actions]);
    };

    // Function to update an actions action
    const updateAction = (action: any, id: any) => helperUpdateAction(id, 'action', action);

    // Function to update an actions values
    const updateValues = (values: any, id: any) => helperUpdateAction(id, 'values', values);

    // Update to pass to the condition components as props
    // (simplifies component definition)
    const updateProps = {
        updateAction,
        updateValues,
        removeAction,
    };

    return (
        <CustomActionBuilderContainer>
            {actions.length === 0 && (
                <EmptyStateContainer>
                    <h3>No Actions Added</h3>
                    <p>Click &apos;Add Action&apos; to get started.</p>
                    <ActionsButtonContainer>
                        <PrimaryButton icon={<Add />} onClick={() => setActions([...actions, defaultAction])}>
                            Add Action
                        </PrimaryButton>
                    </ActionsButtonContainer>
                </EmptyStateContainer>
            )}
            <ActionGroupContainer>
                {actions.map((action) => (
                    <ActionSelector key={action.id} {...action} {...updateProps} />
                ))}
            </ActionGroupContainer>
            {actions.length > 0 && (
                <ActionsButtonContainer>
                    <TextButton icon={<Add />} onClick={() => setActions([...actions, defaultAction])}>
                        Add Action
                    </TextButton>
                </ActionsButtonContainer>
            )}
        </CustomActionBuilderContainer>
    );
};

export const CustomActionSelector = ({ actionSelection, setActionSelection }: any) => {
    const { passing, failing } = actionSelection;

    return (
        <CustomActionsContainer>
            <BuilderGroupContainer>
                <BuilderGroupHeader>
                    <SuccessIcon />
                    <h3>For Passing Assets</h3>
                </BuilderGroupHeader>
                <ActionBuilder selectedActions={passing} updateActions={setActionSelection} />
            </BuilderGroupContainer>
            <BuilderGroupContainer>
                <BuilderGroupHeader>
                    <FailureIcon />
                    <h3>For Failing Assets</h3>
                </BuilderGroupHeader>
                <ActionBuilder selectedActions={failing} updateActions={setActionSelection} />
            </BuilderGroupContainer>
        </CustomActionsContainer>
    );
};
