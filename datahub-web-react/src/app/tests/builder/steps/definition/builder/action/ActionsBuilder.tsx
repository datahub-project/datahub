import React from 'react';
import styled from 'styled-components';
import { DeleteOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { ANTD_GRAY } from '../../../../../../entity/shared/constants';
import { ActionId, ActionType } from '../property/types/action';
import { AddActionButton } from './AddActionButton';
import { Action } from '../../../actions/types';
import { ActionBuilder } from './ActionBuilder';

/**
 * The maximum number of sub-predicates supported in a single
 * clause.
 */
const MAX_ACTIONS = 10;

const Container = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border-radius: 4px;
    padding: 16px;
    border: 0.5px solid ${ANTD_GRAY[3]};
    overflow: auto;
    box-shadow: 0px 0px 6px 0px #e8e8e8;
`;

const ActionWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    margin-bottom: 12px;
`;

const DeleteButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px;
        margin-left: 8px;
    }
`;

const DeleteIcon = styled(DeleteOutlined)`
    && {
        font-size: 14px;
        margin: 0px;
        padding: 0px;
        color: ${ANTD_GRAY[7]};
    }
`;

const EMPTY_ACTION = {
    type: ActionId.ADD_TAGS,
    values: [],
};

type Options = {
    maxActions?: number;
};

type Props = {
    selectedActions: Action[];
    onChangeActions: (newActions: Action[]) => void;
    actionTypes: ActionType[];
    disabled?: boolean;
    options?: Options;
};

/**
 * This component can be used building a set of actions to take against a set of data assets.
 */
export const ActionsBuilder = ({
    selectedActions,
    onChangeActions,
    actionTypes,
    disabled = false,
    options = {
        maxActions: 10,
    },
}: Props) => {
    const onAddAction = () => {
        const newActions = [...selectedActions, EMPTY_ACTION];
        onChangeActions(newActions);
    };

    const onChangeAction = (newAction: Action, index: number) => {
        const newActions = [...selectedActions];
        newActions[index] = newAction;
        onChangeActions(newActions);
    };

    const onRemoveAction = (index: number) => {
        const currentActions = [...selectedActions];
        currentActions.splice(index, 1);
        onChangeActions(currentActions);
    };

    const canAddAction = selectedActions.length < (options?.maxActions || MAX_ACTIONS);

    return (
        <Container>
            {selectedActions.map((action, index) => (
                <ActionWrapper>
                    <ActionBuilder
                        selectedAction={action}
                        actionTypes={actionTypes}
                        onChangeAction={(newAction) => onChangeAction(newAction, index)}
                    />
                    <DeleteButton type="text">
                        <DeleteIcon onClick={() => onRemoveAction(index)} />
                    </DeleteButton>
                </ActionWrapper>
            ))}
            <div>{canAddAction && <AddActionButton disabled={disabled} onAddAction={onAddAction} />}</div>
        </Container>
    );
};
