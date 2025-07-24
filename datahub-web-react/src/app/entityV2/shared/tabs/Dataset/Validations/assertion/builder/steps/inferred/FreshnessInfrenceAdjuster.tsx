import { Collapse, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ExclusionWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/ExclusionWindowAdjuster';
import { InferenceSensitivityAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { LookBackWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import {
    AssertionMonitorBuilderExclusionWindow,
    AssertionMonitorBuilderState,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { useAppConfig } from '@src/app/useAppConfig';

const Row = styled.div`
    display: flex;
    flex-direction: column;
    border-style: solid;
    border-width: 0px;
    border-top-width: 1px;
    border-bottom-width: 1px;
    border-color: #eee;
    margin-top: 24px;
    margin-bottom: 24px;
    padding-top: 16px;
    padding-bottom: 16px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    collapsable?: boolean;
};

export const FreshnessInfrenceAdjuster = (props: Props) => {
    const { state, updateState, disabled, collapsable } = props;

    const { inferenceSettings } = state;
    const { sensitivity, exclusionWindows, trainingDataLookbackWindowDays } = inferenceSettings || {};

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
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
                        inferenceSettings: {
                            ...inferenceSettings,
                            sensitivity: { level: value },
                        },
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
        <Row style={collapsable ? { marginBottom: 12 } : {}}>
            {collapsable ? (
                <Collapse>
                    <Collapse.Panel header="AI Model Tuning" key="ai-model-tuning">
                        {inferenceContent}
                    </Collapse.Panel>
                </Collapse>
            ) : (
                inferenceContent
            )}
        </Row>
    );
};
