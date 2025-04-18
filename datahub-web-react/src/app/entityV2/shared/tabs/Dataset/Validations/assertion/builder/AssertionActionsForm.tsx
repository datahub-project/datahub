import React from 'react';
import styled from 'styled-components';
import { Checkbox, Typography } from 'antd';
import { AssertionActionType } from '../../../../../../../../types.generated';
import { toggleRaiseIncidentState, toggleResolveIncidentState } from './utils';
import { AssertionActionsFormState } from './types';

const Form = styled.div``;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

const StyledCheckbox = styled(Checkbox)`
    margin-top: 8px;
`;

type Props = {
    state: AssertionActionsFormState;
    updateState: (newState: AssertionActionsFormState) => void;
};

/**
 * Form for editing the actions that are taken on success or failure of an Assertion
 */
export const AssertionActionsForm = ({ state, updateState }: Props) => {
    const raiseIncidents = state?.onFailure?.some((action) => action.type === AssertionActionType.RaiseIncident);
    const resolveIncidents = state?.onSuccess?.some((action) => action.type === AssertionActionType.ResolveIncident);

    return (
        <Form>
            <Section>
                <Typography.Title level={5}>If this assertion fails...</Typography.Title>
                <StyledCheckbox
                    checked={raiseIncidents}
                    onChange={(e) => updateState(toggleRaiseIncidentState(state, e.target.checked as boolean))}
                >
                    Auto-raise incident
                </StyledCheckbox>
            </Section>
            <Section>
                <Typography.Title level={5}>If this assertion passes...</Typography.Title>
                <StyledCheckbox
                    checked={resolveIncidents}
                    onChange={(e) => updateState(toggleResolveIncidentState(state, e.target.checked as boolean))}
                >
                    Auto-resolve active incident
                </StyledCheckbox>
            </Section>
        </Form>
    );
};
