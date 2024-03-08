import React from 'react';

import { Collapse } from 'antd';
import styled from 'styled-components';

import { AssertionMonitorBuilderState } from '../../types';
import {
    AssertionStdParameters,
    AssertionType,
    CronSchedule,
    DatasetFilter,
    DatasetVolumeSourceType,
    VolumeAssertionInfo,
} from '../../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../freshness/EvaluationScheduleBuilder';
import { VolumeTypeBuilder } from './VolumeTypeBuilder';
import { VolumeParametersBuilder } from './VolumeParametersBuilder';
import { VolumeSourceTypeBuilder } from './VolumeSourceTypeBuilder';
import { VolumeFilterBuilder } from './VolumeFilterBuilder';
import { AssertionActionsSection } from '../actions/AssertionActionsSection';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    editing?: boolean;
};

export const VolumeAssertionBuilder = ({ state, updateState, editing }: Props) => {
    const assertion = state?.assertion;
    const schedule: CronSchedule | null | undefined = state?.schedule;
    const volumeAssertion = assertion?.volumeAssertion;
    const volumeParameters = volumeAssertion?.parameters;
    const filter = volumeAssertion?.filter;
    const sourceType = state.parameters?.datasetVolumeParameters?.sourceType;
    const entityUrn = state?.entityUrn as string;
    const platformUrn = state?.platformUrn as string;

    const updateAssertionSchedule = (newSchedule: CronSchedule) => {
        updateState({
            ...state,
            schedule: newSchedule,
        });
    };

    const updateVolumeType = (newVolumeAssertion: Partial<VolumeAssertionInfo>) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...newVolumeAssertion,
                },
            },
        });
    };

    const updateVolumeParameters = (newVolumeParameters: AssertionStdParameters) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...volumeAssertion,
                    parameters: newVolumeParameters,
                },
            },
        });
    };

    const updateSourceType = (newSourceType: DatasetVolumeSourceType) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...volumeAssertion,
                    filter: undefined, // Reset filter when source type changes
                },
            },
            parameters: {
                ...state.parameters,
                datasetVolumeParameters: {
                    ...state.parameters?.datasetVolumeParameters,
                    sourceType: newSourceType,
                },
            },
        });
    };

    const updateFilter = (newFilter?: DatasetFilter) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...volumeAssertion,
                    filter: newFilter,
                },
            },
        });
    };

    const updateVolumeAssertion = (newVolumeAssertion: Partial<VolumeAssertionInfo>) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                volumeAssertion: {
                    ...state.assertion?.volumeAssertion,
                    ...newVolumeAssertion,
                },
            },
        });
    };

    return (
        <div>
            <EvaluationScheduleBuilder
                value={schedule}
                assertionType={AssertionType.Volume}
                onChange={updateAssertionSchedule}
                disabled={!editing}
            />
            <VolumeTypeBuilder volumeInfo={volumeAssertion as VolumeAssertionInfo} onChange={updateVolumeType} disabled={!editing} />
            <VolumeParametersBuilder
                volumeInfo={volumeAssertion as VolumeAssertionInfo}
                value={volumeParameters as AssertionStdParameters}
                onChange={updateVolumeParameters}
                updateVolumeAssertion={updateVolumeAssertion}
                disabled={!editing}
            />
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <VolumeSourceTypeBuilder
                            entityUrn={entityUrn}
                            platformUrn={platformUrn}
                            value={sourceType as DatasetVolumeSourceType}
                            onChange={updateSourceType}
                            disabled={!editing}
                        />
                        <VolumeFilterBuilder
                            value={filter as DatasetFilter}
                            onChange={updateFilter}
                            sourceType={sourceType as DatasetVolumeSourceType}
                            disabled={!editing}
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>
            <AssertionActionsSection state={state} updateState={updateState} editing={editing} />
        </div>
    );
};
