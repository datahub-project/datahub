import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Checkbox, Typography } from 'antd';
import { StepProps } from '../types';
import { AssertionActionType } from '../../../../../../../../../types.generated';
import { toggleRaiseIncidentState, toggleResolveIncidentState } from './utils';

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Form = styled.div``;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

const StyledCheckbox = styled(Checkbox)`
    margin-top: 8px;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

/**
 * Step for configuring the actions for an assertion, i.e. what happens when it fails
 */
export const ConfigureActionsStep = ({ state, updateState, prev, submit }: StepProps) => {
    const [isSubmitting, setSubmitting] = useState(false);
    const actions = state.assertion?.actions;

    const raiseIncidents =
        (actions?.onFailure?.filter((action) => action.type === AssertionActionType.RaiseIncident)?.length || 0) > 0;
    const resolveIncidents =
        (actions?.onSuccess?.filter((action) => action.type === AssertionActionType.ResolveIncident)?.length || 0) > 0;

    return (
        <Step>
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
            <ControlsContainer>
                <Button onClick={prev}>Back</Button>
                <Button
                    type="primary"
                    onClick={async () => {
                        try {
                            setSubmitting(true);
                            await submit();
                        } finally {
                            setSubmitting(false);
                        }
                    }}
                    disabled={isSubmitting}
                >
                    Save
                </Button>
            </ControlsContainer>
        </Step>
    );
};
