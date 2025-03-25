import React, { useEffect } from 'react';

import styled from 'styled-components';
import { Typography } from 'antd';
import TextArea from 'antd/lib/input/TextArea';

import {
    AssertionMonitorBuilderState,
    FieldMetricAssertionBuilderOperatorOptions,
    FreshnessAssertionScheduleBuilderTypeOptions,
    VolumeAssertionBuilderTypeOptions,
} from '../../types';
import { AssertionType, FieldAssertionType } from '../../../../../../../../../../types.generated';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    onValidityChange?: (isValid: boolean) => void;
};

/**
 * Final step in assertion creation flow: Give it a name / description.
 */
export const FinishUpBuilder = ({ state, updateState, onValidityChange }: Props) => {
    const description = state.assertion?.description;

    const isSQLAssertion = state.assertion?.type === AssertionType.Sql;
    const isVolumeAIInferredAssertion =
        state.assertion?.type === AssertionType.Volume &&
        state.assertion?.volumeAssertion?.type === VolumeAssertionBuilderTypeOptions.AiInferredRowCountTotal;
    const isFreshnessAIInferredAssertion =
        state.assertion?.type === AssertionType.Freshness &&
        state.assertion?.freshnessAssertion?.schedule?.type === FreshnessAssertionScheduleBuilderTypeOptions.AiInferred;
    const isFieldMetricAIInferredAssertion =
        state.assertion?.type === AssertionType.Field &&
        state.assertion?.fieldAssertion?.type === FieldAssertionType.FieldMetric &&
        state.assertion?.fieldAssertion?.fieldMetricAssertion?.operator ===
            FieldMetricAssertionBuilderOperatorOptions.AiInferred;
    const isDescriptionRequired =
        isSQLAssertion ||
        isVolumeAIInferredAssertion ||
        isFreshnessAIInferredAssertion ||
        isFieldMetricAIInferredAssertion;

    useEffect(() => {
        onValidityChange?.(!isDescriptionRequired || !!description?.length);
    }, [description, isDescriptionRequired, onValidityChange]);

    const updateDescription = (newDescription: string) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                description: newDescription,
            },
        });
    };

    return (
        <div>
            <Section>
                <Typography.Title level={5}>Name</Typography.Title>
                <TextArea
                    value={description || ''}
                    placeholder={`Give this assertion a name${isDescriptionRequired ? '' : ' (optional)'}`}
                    onChange={(e) => updateDescription(e.target.value)}
                />
                <Typography.Paragraph style={{ marginTop: 4 }} type="secondary">
                    {isDescriptionRequired
                        ? 'Required for this assertion type.'
                        : 'If not specified, a name will be generated from the assertion settings.'}
                </Typography.Paragraph>
            </Section>
        </div>
    );
};
