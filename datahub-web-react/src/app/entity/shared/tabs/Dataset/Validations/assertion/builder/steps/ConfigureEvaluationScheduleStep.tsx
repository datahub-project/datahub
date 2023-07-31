import React, { useEffect } from 'react';
import styled from 'styled-components';
import { Button, Checkbox, Collapse, Typography } from 'antd';
import { StepProps } from '../types';
import {
    AssertionActionType,
    AssertionType,
    CronSchedule,
    FreshnessAssertionScheduleType,
} from '../../../../../../../../../types.generated';
import { CronScheduleBuilder } from './freshness/CronScheduleBuilder';
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
 * Step for defining the schedule + actions for an assertion
 */
export const ConfigureEvaluationScheduleStep = ({ state, updateState, prev, submit }: StepProps) => {
    const showSchedule = state.assertion?.freshnessAssertion?.schedule?.type !== FreshnessAssertionScheduleType.Cron;
    const actions = state.assertion?.actions;

    const raiseIncidents =
        (actions?.onFailure?.filter((action) => action.type === AssertionActionType.RaiseIncident)?.length || 0) > 0;
    const resolveIncidents =
        (actions?.onSuccess?.filter((action) => action.type === AssertionActionType.ResolveIncident)?.length || 0) > 0;

    const updateAssertionSchedule = (cronSchedule: CronSchedule) => {
        updateState({
            ...state,
            schedule: cronSchedule,
        });
    };

    /**
     * Auto-Configure the Schedule for any Freshness Assertions that leverage a cron schedule.
     * This is because CRON schedules should be executed on their schedules,
     * as it does not make sense to evaluate a cron-schedule assertion on some fixed cadence.
     */
    useEffect(() => {
        if (!state.schedule) {
            if (state.assertion?.type === AssertionType.Freshness) {
                const cronSchedule = state.assertion?.freshnessAssertion?.schedule?.cron;
                if (cronSchedule) {
                    updateState({
                        ...state,
                        schedule: cronSchedule,
                    });
                }
            }
        }
    }, [state, updateState]);

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
                <Section>
                    {showSchedule && (
                        <Collapse>
                            <Collapse.Panel key="Advanced" header="Advanced - Evaluation Schedule">
                                <CronScheduleBuilder
                                    title="When should this assertion be evaluated?"
                                    value={state.schedule as CronSchedule}
                                    onChange={updateAssertionSchedule}
                                />
                            </Collapse.Panel>
                        </Collapse>
                    )}
                </Section>
            </Form>
            <ControlsContainer>
                <Button onClick={prev}>Back</Button>
                <Button type="primary" onClick={submit}>
                    Save
                </Button>
            </ControlsContainer>
        </Step>
    );
};
