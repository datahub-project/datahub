import { Checkbox, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    toggleRaiseIncidentState,
    toggleResolveIncidentState,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionActionType } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

const StyledCheckbox = styled(Checkbox)`
    margin-top: 8px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

/**
 * Form for editing the actions that are taken on success or failure of an Assertion
 */
export const AssertionActionsSection = ({ state, updateState, disabled = false }: Props) => {
    const raiseIncidents = state?.assertion?.actions?.onFailure?.some(
        (action) => action.type === AssertionActionType.RaiseIncident,
    );
    const resolveIncidents = state?.assertion?.actions?.onSuccess?.some(
        (action) => action.type === AssertionActionType.ResolveIncident,
    );

    return (
        <div style={{ marginTop: 16, marginBottom: 24 }}>
            <Section>
                <Typography.Title level={5}>If this assertion fails...</Typography.Title>
                <StyledCheckbox
                    disabled={disabled}
                    checked={raiseIncidents}
                    onChange={(e) => updateState(toggleRaiseIncidentState(state, e.target.checked as boolean))}
                >
                    Auto-raise incident
                </StyledCheckbox>
            </Section>
            <Section>
                <Typography.Title level={5}>If this assertion passes...</Typography.Title>
                <StyledCheckbox
                    disabled={disabled}
                    checked={resolveIncidents}
                    onChange={(e) => updateState(toggleResolveIncidentState(state, e.target.checked as boolean))}
                >
                    Auto-resolve active incident
                </StyledCheckbox>
            </Section>
        </div>
    );
};
