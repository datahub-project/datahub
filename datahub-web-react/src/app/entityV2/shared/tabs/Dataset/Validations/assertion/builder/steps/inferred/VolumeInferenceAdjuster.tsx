import { Collapse, Typography } from 'antd';
import React, { forwardRef, useImperativeHandle, useRef } from 'react';
import styled from 'styled-components';

import { EvaluationScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { ExclusionWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/ExclusionWindowAdjuster';
import { FuturePredictionsList } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/FuturePredictionsList';
import { InferenceSensitivityAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { LookBackWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import {
    AssertionMonitorBuilderExclusionWindow,
    AssertionMonitorBuilderState,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { useAppConfig } from '@src/app/useAppConfig';
import { AssertionType, CronSchedule } from '@src/types.generated';

const Row = styled.div`
    display: flex;
    flex-direction: column;
    border-style: solid;
    border-width: 0px;
    border-top-width: 1px;
    border-bottom-width: 0;
    border-color: #eee;
    margin-top: 24px;
    margin-bottom: 24px;
    padding-top: 24px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    onSave?: () => void;
    collapsable?: boolean;
};

export interface VolumeInferenceAdjusterHandle {
    triggerRegeneration: () => void;
}

export const VolumeInferenceAdjuster = forwardRef<VolumeInferenceAdjusterHandle, Props>((props, ref) => {
    const { state, updateState, disabled, collapsable } = props;
    const futurePredictionsRef = useRef<VolumeInferenceAdjusterHandle>(null);

    const { inferenceSettings, schedule } = state;
    const { sensitivity, trainingDataLookbackWindowDays, exclusionWindows } = inferenceSettings || {};

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;

    // Forward the ref to the FuturePredictionsList
    useImperativeHandle(ref, () => ({
        triggerRegeneration: () => {
            if (futurePredictionsRef.current) {
                futurePredictionsRef.current.triggerRegeneration();
            }
        },
    }));

    if (!onlineSmartAssertionsEnabled) return null;

    const inferenceContent = (
        <>
            {/* Title - only show if not collapsable since Collapse will have its own title */}
            {!collapsable && <Typography.Title level={5}>AI Model Tuning</Typography.Title>}

            {/* Sensitivity */}
            <InferenceSensitivityAdjuster
                sensitivity={sensitivity?.level}
                disabled={disabled}
                onChange={(value) => {
                    updateState({
                        ...state,
                        inferenceSettings: { ...inferenceSettings, sensitivity: { level: value } },
                    });
                }}
            />

            {/* Exclusion windows */}
            <ExclusionWindowAdjuster
                exclusionWindows={exclusionWindows || []}
                disabled={disabled}
                onChange={(value: AssertionMonitorBuilderExclusionWindow) => {
                    updateState({ ...state, inferenceSettings: { ...inferenceSettings, exclusionWindows: value } });
                }}
            />

            {/* Training data lookback window days */}
            <LookBackWindowAdjuster
                trainingDataLookbackWindowDays={trainingDataLookbackWindowDays}
                disabled={disabled}
                onChange={(value) => {
                    updateState({
                        ...state,
                        inferenceSettings: { ...inferenceSettings, trainingDataLookbackWindowDays: value },
                    });
                }}
            />
        </>
    );

    return (
        <Row style={collapsable ? { marginBottom: 0, marginTop: 0 } : {}}>
            {/* Future Predictions - Only show in edit mode, always outside the accordion */}
            {state.assertion?.urn && (
                <FuturePredictionsList state={state} ref={futurePredictionsRef} onSave={props.onSave} />
            )}

            {collapsable ? (
                <Collapse>
                    <Collapse.Panel header="AI Model Tuning" key="ai-model-tuning">
                        {inferenceContent}
                    </Collapse.Panel>
                </Collapse>
            ) : (
                inferenceContent
            )}

            {/* Schedule - always outside the accordion */}
            <EvaluationScheduleBuilder
                value={schedule}
                assertionType={AssertionType.Volume}
                onChange={(newSchedule: CronSchedule) => {
                    updateState({
                        ...state,
                        schedule: newSchedule,
                    });
                }}
                disabled={disabled}
            />
        </Row>
    );
});
