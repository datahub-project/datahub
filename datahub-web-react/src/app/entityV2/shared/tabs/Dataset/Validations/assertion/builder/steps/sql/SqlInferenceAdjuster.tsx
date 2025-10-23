import { colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ExclusionWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/ExclusionWindowAdjuster';
import { InferenceSensitivityAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
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
    border-bottom-width: 0;
    border-color: #eee;
    margin-top: 24px;
    margin-bottom: 24px;
    padding-top: 24px;
`;

const TitleWrapper = styled.div`
    display: flex;
    flex-direction: column;
    margin-bottom: 12px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
};

export const SqlInferenceAdjuster = ({ state, updateState, disabled, isEditMode }: Props) => {
    const { inferenceSettings } = state;
    const { sensitivity, exclusionWindows } = inferenceSettings || {};

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;

    if (!onlineSmartAssertionsEnabled) return null;

    return (
        <Row>
            <TitleWrapper>
                <Typography.Title level={5}>AI Model Tuning</Typography.Title>
                {!isEditMode && (
                    <Typography.Text style={{ color: colors.gray[400] }}>
                        Consider tuning this after the assertion is up and running.
                    </Typography.Text>
                )}
            </TitleWrapper>

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
                    updateState({
                        ...state,
                        inferenceSettings: { ...inferenceSettings, exclusionWindows: value },
                    });
                }}
            />
        </Row>
    );
};
