import React, { useState } from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { AssertionActionsBuilderState } from './types';
import { Assertion } from '../../../../../../../../types.generated';
import { DEFAULT_ACTIONS_BUILDER_STATE } from './constants';
import { AssertionActionsForm } from './AssertionActionsForm';
import { useUpdateAssertionActions } from './useUpdateAssertionActions';

const Container = styled.div`
    display: flex;
    align-items: top;
    justify-content: space-between;
    flex-direction: column;
    width: 100%;
    min-height: 50vh;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const assertionToBuilderState = (assertion: Assertion) => {
    if (assertion.actions) {
        return {
            actions: {
                onSuccess:
                    assertion.actions?.onSuccess?.map((action) => ({
                        type: action.type,
                    })) || [],
                onFailure:
                    assertion.actions?.onFailure?.map((action) => ({
                        type: action.type,
                    })) || [],
            },
        };
    }
    return DEFAULT_ACTIONS_BUILDER_STATE;
};

type Props = {
    urn: string;
    assertion: Assertion;
    onSubmit?: (assertion: Assertion) => void;
    onCancel?: () => void;
};

export const AssertionActionsBuilder = ({ urn, assertion, onSubmit, onCancel }: Props) => {
    const [builderState, setBuilderState] = useState<AssertionActionsBuilderState>(assertionToBuilderState(assertion));

    const onUpdatedAssertionActions = (newAssertion: Assertion) => {
        onSubmit?.(newAssertion);
        setBuilderState(DEFAULT_ACTIONS_BUILDER_STATE);
    };

    const updateAssertionActions = useUpdateAssertionActions(urn, builderState, onUpdatedAssertionActions);

    return (
        <Container>
            <AssertionActionsForm
                state={{ onSuccess: builderState.actions?.onSuccess || [], onFailure: builderState.actions?.onFailure }}
                updateState={(newState) => setBuilderState({ actions: newState })}
            />
            <ControlsContainer>
                <Button onClick={onCancel}>Cancel</Button>
                <Button type="primary" onClick={updateAssertionActions}>
                    Save
                </Button>
            </ControlsContainer>
        </Container>
    );
};
