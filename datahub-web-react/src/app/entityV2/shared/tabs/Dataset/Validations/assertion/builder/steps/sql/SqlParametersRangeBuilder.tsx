import { Form, InputNumber, Select, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { SQL_CHANGE_TYPE_OPTIONS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionStdParameterType, AssertionValueChangeType, SqlAssertionType } from '@types';

const StyledSelect = styled(Select)`
    width: 120px;
`;

const InputGroup = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
    flex-wrap: wrap;
`;

const StyledFormItem = styled(Form.Item)<{ $hasAddonBefore?: boolean }>`
    width: ${(props) => (props.$hasAddonBefore ? '220px' : '100px')};
    margin: 0;

    .ant-input-number-group-addon {
        font-size: 12px;
    }

    .ant-input-number {
        width: 100%;
    }

    .ant-input-number-input {
        padding-left: 8px;
        padding-right: 8px;
        text-align: left;
    }
`;

const StyledNumberInput = styled(InputNumber)`
    width: 100%;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    updateMetricChange: (newChangeType: AssertionValueChangeType) => void;
    disabled?: boolean;
};

export const SqlParametersRangeBuilder = ({ value, onChange, updateMetricChange, disabled }: Props) => {
    const isMetricChange = value.assertion?.sqlAssertion?.type === SqlAssertionType.MetricChange;
    const changeType = value.assertion?.sqlAssertion?.changeType;
    const isPercentage = changeType === AssertionValueChangeType.Percentage;

    const updateMinValue = (newValue: string | number | null) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                sqlAssertion: {
                    ...value.assertion?.sqlAssertion,
                    parameters: {
                        ...value.assertion?.sqlAssertion?.parameters,
                        minValue: {
                            type: AssertionStdParameterType.Number,
                            value: (newValue || 0).toString(),
                        },
                    },
                },
            },
        });
    };

    const updateMaxValue = (newValue: string | number | null) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                sqlAssertion: {
                    ...value.assertion?.sqlAssertion,
                    parameters: {
                        ...value.assertion?.sqlAssertion?.parameters,
                        maxValue: {
                            type: AssertionStdParameterType.Number,
                            value: (newValue || 0).toString(),
                        },
                    },
                },
            },
        });
    };

    const minValue = value.assertion?.sqlAssertion?.parameters?.minValue?.value;
    const maxValue = value.assertion?.sqlAssertion?.parameters?.maxValue?.value;

    return (
        <InputGroup>
            <Typography.Text strong>is at least</Typography.Text>
            <StyledFormItem
                $hasAddonBefore={isMetricChange}
                initialValue={minValue}
                name="sqlParameters.minValue"
                rules={[
                    { required: true, message: 'Required' },
                    ({ getFieldValue }) => ({
                        validator(_, fieldValue) {
                            if (fieldValue >= getFieldValue('sqlParameters.maxValue')) {
                                return Promise.reject(new Error('Must be less than maximum'));
                            }
                            return Promise.resolve();
                        },
                    }),
                ]}
            >
                <StyledNumberInput
                    value={minValue}
                    onChange={updateMinValue}
                    disabled={disabled}
                    controls={false}
                    addonAfter={isPercentage ? '%' : undefined}
                    addonBefore={
                        isMetricChange ? (
                            <StyledSelect
                                value={changeType}
                                onChange={(newChangeType) =>
                                    updateMetricChange(newChangeType as AssertionValueChangeType)
                                }
                                options={SQL_CHANGE_TYPE_OPTIONS}
                                disabled={disabled}
                            />
                        ) : undefined
                    }
                />
            </StyledFormItem>
            <Typography.Text strong>and at most</Typography.Text>
            <StyledFormItem
                $hasAddonBefore={isMetricChange}
                name="sqlParameters.maxValue"
                initialValue={maxValue}
                rules={[
                    { required: true, message: 'Required' },
                    ({ getFieldValue }) => ({
                        validator(_, fieldValue) {
                            if (fieldValue <= getFieldValue('sqlParameters.minValue')) {
                                return Promise.reject(new Error('Must be greater than minimum'));
                            }
                            return Promise.resolve();
                        },
                    }),
                ]}
            >
                <StyledNumberInput
                    value={maxValue}
                    onChange={updateMaxValue}
                    disabled={disabled}
                    controls={false}
                    addonAfter={isPercentage ? '%' : undefined}
                    addonBefore={
                        isMetricChange ? (
                            <StyledSelect
                                value={changeType}
                                onChange={(newChangeType) =>
                                    updateMetricChange(newChangeType as AssertionValueChangeType)
                                }
                                options={SQL_CHANGE_TYPE_OPTIONS}
                                disabled={disabled}
                            />
                        ) : undefined
                    }
                />
            </StyledFormItem>
        </InputGroup>
    );
};
