import { Button, Icon, Input, SimpleSelect, Text, Tooltip, colors } from '@components';
import { Col, DatePicker, Row, message } from 'antd';
import moment from 'moment';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { DEFAULT_SMART_ASSERTION_SENSITIVITY } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { removeNestedTypeNames } from '@app/shared/subscribe/drawer/utils';

import { AssertionAdjustmentSettings, AssertionExclusionWindow, AssertionExclusionWindowType } from '@types';

const { RangePicker } = DatePicker;

const StyledPanel = styled.div`
    padding: 24px 0;
    border-top: 1px solid ${colors.gray[100]};
`;

const SettingsSection = styled.div`
    margin-bottom: 24px;
`;

const ExclusionWindowItem = styled.div<{ $isDisabled?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 3px 8px;
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    margin-bottom: 8px;
    background-color: ${(props) => (props.$isDisabled ? colors.gray[1500] : colors.gray[50])};
    cursor: ${(props) => (props.$isDisabled ? 'not-allowed' : 'default')};
`;

const SectionTitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

const ExclusionWindowsContainer = styled.div`
    margin-bottom: 12px;
`;

const HelpIcon = styled(Icon)`
    cursor: help;
    color: ${colors.gray[500]};

    &:hover {
        color: ${colors.gray[700]};
    }
`;

const StyledRangePickerWrapper = styled.div`
    &&& .ant-picker-disabled {
        background-color: ${colors.gray[1500]};
        color: ${colors.gray[300]};
        cursor: not-allowed;

        .ant-picker-input > input {
            color: ${colors.gray[300]};
        }

        .ant-picker-separator {
            color: ${colors.gray[300]};
        }

        .ant-picker-suffix {
            color: ${colors.gray[300]};
        }
    }
`;

type Props = {
    onUpdateSettings: (settings: AssertionAdjustmentSettings) => Promise<void>;
    inferenceSettings?: AssertionAdjustmentSettings;
    isUpdating?: boolean;
};

export const MonitorInferenceSettingsControlPanel = ({
    onUpdateSettings,
    inferenceSettings,
    isUpdating = false,
}: Props) => {
    const trainingLookbackWindowDays =
        inferenceSettings?.trainingDataLookbackWindowDays ?? DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS;

    const exclusionWindows: AssertionExclusionWindow[] = useMemo(
        () => inferenceSettings?.exclusionWindows ?? [],
        [inferenceSettings],
    );

    const sensitivity = useMemo(
        () => inferenceSettings?.sensitivity?.level ?? DEFAULT_SMART_ASSERTION_SENSITIVITY,
        [inferenceSettings],
    );

    const [showDatePicker, setShowDatePicker] = useState(false);
    const [lookbackDaysInput, setLookbackDaysInput] = useState(trainingLookbackWindowDays.toString());

    // Sync local state with prop changes
    useEffect(() => {
        setLookbackDaysInput(trainingLookbackWindowDays.toString());
    }, [trainingLookbackWindowDays]);

    const handleSensitivityChange = useCallback(
        async (value: number) => {
            if (value === inferenceSettings?.sensitivity?.level) return;
            try {
                const existingSettings = removeNestedTypeNames(inferenceSettings);
                await onUpdateSettings({
                    ...existingSettings,
                    sensitivity: { level: value },
                });
                message.success('Sensitivity updated successfully');
            } catch (error) {
                console.error('Error updating sensitivity:', error);
                message.error('Failed to update sensitivity');
            }
        },
        [onUpdateSettings, inferenceSettings],
    );

    const handleLookbackDaysChange = useCallback(
        async (value: number | null) => {
            if (value === null || value < 1 || value === inferenceSettings?.trainingDataLookbackWindowDays) return;

            try {
                const existingSettings = removeNestedTypeNames(inferenceSettings);
                await onUpdateSettings({
                    ...existingSettings,
                    trainingDataLookbackWindowDays: value,
                });
                message.success('Maximum lookback days updated successfully');
            } catch (error) {
                console.error('Error updating lookback days:', error);
                message.error('Failed to update maximum lookback days');
            }
        },
        [onUpdateSettings, inferenceSettings],
    );

    const handleDateRangeSelect = useCallback(
        async (dates: [moment.Moment | null, moment.Moment | null] | null) => {
            if (!dates || !dates[0] || !dates[1]) {
                setShowDatePicker(false);
                return;
            }

            const startTimeMillis = dates[0].valueOf();
            const endTimeMillis = dates[1].valueOf();

            const displayName = `${dates[0].format('MMM D, YYYY h:mm A')} - ${dates[1].format('MMM D, YYYY h:mm A')}`;

            const newWindow: AssertionExclusionWindow = {
                type: AssertionExclusionWindowType.FixedRange,
                displayName,
                fixedRange: { startTimeMillis, endTimeMillis },
            };

            try {
                const existingSettings = removeNestedTypeNames(inferenceSettings);
                await onUpdateSettings({
                    ...existingSettings,
                    exclusionWindows: [...(existingSettings?.exclusionWindows ?? []), newWindow],
                });
                setShowDatePicker(false);
            } catch (error) {
                console.error('Error adding exclusion window:', error);
                setShowDatePicker(false);
            }
        },
        [onUpdateSettings, inferenceSettings],
    );

    const handleRemoveExclusionWindow = useCallback(
        async (indexToRemove: number) => {
            try {
                const existingSettings = removeNestedTypeNames(inferenceSettings);
                const updatedWindows =
                    existingSettings?.exclusionWindows?.filter((_, index) => index !== indexToRemove) ?? [];
                await onUpdateSettings({
                    ...existingSettings,
                    exclusionWindows: updatedWindows,
                });
            } catch (error) {
                console.error('Error removing exclusion window:', error);
            }
        },
        [onUpdateSettings, inferenceSettings],
    );

    return (
        <StyledPanel>
            <Row gutter={32}>
                {/* Left Column - Exclusion Windows */}
                <Col span={12}>
                    <SettingsSection>
                        <SectionTitleContainer>
                            <Text size="sm" weight="bold" color="gray" colorLevel={600}>
                                Exclusion Windows
                            </Text>
                            <Tooltip title="Define specific date and time periods when assertion monitoring should be disabled. Useful for maintenance windows, holidays, or known data processing delays.">
                                <HelpIcon icon="Question" source="phosphor" size="sm" />
                            </Tooltip>
                        </SectionTitleContainer>

                        {/* Existing Exclusion Windows */}
                        <ExclusionWindowsContainer>
                            {exclusionWindows.map((window, index) => (
                                <ExclusionWindowItem key={window.fixedRange?.startTimeMillis} $isDisabled={isUpdating}>
                                    <Text color="gray" size="sm" colorLevel={600} weight="medium">
                                        {window.displayName || 'Exclusion Window'}
                                    </Text>
                                    <Button
                                        variant="text"
                                        size="sm"
                                        icon={{ icon: 'Trash', source: 'phosphor', color: 'red' }}
                                        onClick={() => handleRemoveExclusionWindow(index)}
                                        disabled={isUpdating}
                                    />
                                </ExclusionWindowItem>
                            ))}
                        </ExclusionWindowsContainer>

                        {/* Add New Exclusion Window */}
                        {showDatePicker ? (
                            <StyledRangePickerWrapper>
                                <RangePicker
                                    open
                                    value={null}
                                    onChange={handleDateRangeSelect}
                                    placeholder={['Start Date & Time', 'End Date & Time']}
                                    style={{ width: '100%' }}
                                    disabled={isUpdating}
                                    onBlur={() => setShowDatePicker(false)}
                                    autoFocus
                                    showTime={{
                                        format: 'HH:mm',
                                        defaultValue: [moment('00:00', 'HH:mm'), moment('23:59', 'HH:mm')],
                                    }}
                                    format="MMM D, YYYY HH:mm"
                                />
                            </StyledRangePickerWrapper>
                        ) : (
                            <Button
                                variant="text"
                                icon={{ icon: 'Plus', source: 'phosphor' }}
                                onClick={() => setShowDatePicker(true)}
                                disabled={isUpdating}
                                style={{ width: '100%' }}
                            >
                                Exclusion Window
                            </Button>
                        )}
                    </SettingsSection>
                </Col>

                {/* Right Column - Sensitivity & Lookback Days */}
                <Col span={12}>
                    {/* Sensitivity */}
                    <SettingsSection>
                        <SectionTitleContainer>
                            <Text size="sm" weight="bold" color="gray" colorLevel={600}>
                                Sensitivity
                            </Text>
                            <Tooltip title="Set how tight the predicted assertion bounds will be. High sensitivity will result in a tighter fit and potentially more false positive alerts, while low sensitivity will be looser and can result in fewer alerts.">
                                <HelpIcon icon="Question" source="phosphor" size="sm" />
                            </Tooltip>
                        </SectionTitleContainer>

                        <SimpleSelect
                            values={[sensitivity.toString()]}
                            onUpdate={(values) => handleSensitivityChange(parseInt(values[0], 10))}
                            isDisabled={isUpdating}
                            width="full"
                            options={[
                                { label: 'High (10)', value: '10' },
                                { label: 'Medium (5)', value: '5' },
                                { label: 'Low (1)', value: '1' },
                            ]}
                            showClear={false}
                        />
                    </SettingsSection>

                    {/* Training Lookback Window */}
                    <SettingsSection>
                        <SectionTitleContainer>
                            <Text size="sm" weight="bold" color="gray" colorLevel={600}>
                                Maximum Training Data Days
                            </Text>
                            <Tooltip title="Number of days of historical data to use for training the smart assertion model. More days provide better seasonal detection but can be more susceptible to outliers.">
                                <HelpIcon icon="Question" source="phosphor" size="sm" />
                            </Tooltip>
                        </SectionTitleContainer>

                        <Input
                            label=""
                            value={lookbackDaysInput}
                            setValue={setLookbackDaysInput}
                            onBlur={() => {
                                const numValue = parseInt(lookbackDaysInput, 10);
                                if (!Number.isNaN(numValue) && numValue >= 1 && numValue <= 365) {
                                    handleLookbackDaysChange(numValue);
                                } else {
                                    // Reset to current valid value if invalid
                                    setLookbackDaysInput(trainingLookbackWindowDays.toString());
                                }
                            }}
                            min={7}
                            max={365}
                            isDisabled={isUpdating}
                            placeholder="Enter number of days (7-365)"
                            type="number"
                        />
                    </SettingsSection>
                </Col>
            </Row>
        </StyledPanel>
    );
};
