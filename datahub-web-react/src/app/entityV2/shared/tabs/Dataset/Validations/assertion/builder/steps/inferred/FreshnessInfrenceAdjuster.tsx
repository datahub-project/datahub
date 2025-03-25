import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { useAppConfig } from '@src/app/useAppConfig';
import { AssertionMonitorBuilderExclusionWindow, AssertionMonitorBuilderState } from '../../types';
import { InferenceSensitivityAdjuster } from './common/InferenceSensitivityAdjuster';
import { LookBackWindowAdjuster } from './common/LookBackWindowAdjuster';
import { ExclusionWindowAdjuster } from './common/ExclusionWindowAdjuster';

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
};

export const FreshnessInfrenceAdjuster = (props: Props) => {
    const { state, updateState, disabled } = props;

    const { inferenceSettings } = state;
    const { sensitivity, exclusionWindows, trainingDataLookbackWindowDays } = inferenceSettings || {};

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
    if (!onlineSmartAssertionsEnabled) return null;

    return (
        <Row>
            {/* Title */}
            <Typography.Title level={5}>Inference Settings</Typography.Title>

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
        </Row>
    );
};
