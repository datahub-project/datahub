import React from 'react';
import { Form, InputNumber, Select, Typography } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../types';
import {
    AssertionStdParameterType,
    AssertionValueChangeType,
    SqlAssertionType,
} from '../../../../../../../../../../types.generated';
import { SQL_CHANGE_TYPE_OPTIONS } from './utils';

const StyledSelect = styled(Select)`
    width: 120px;
`;

const InputGroup = styled.div`
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const StyledFormItem = styled(Form.Item)`
    display: inline-block;
    margin: 0;
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

    const updateMinValue = (newValue: string | null) => {
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

    const updateMaxValue = (newValue: string | null) => {
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
    return (
        <InputGroup>
            <Typography.Text strong>if less than</Typography.Text>
            <StyledFormItem
                name="sqlParameters.minValue"
                initialValue={value.assertion?.sqlAssertion?.parameters?.minValue?.value}
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
                <InputNumber
                    value={value.assertion?.sqlAssertion?.parameters?.minValue?.value}
                    onChange={updateMinValue}
                    disabled={disabled}
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
            <Typography.Text strong>or more than</Typography.Text>
            <StyledFormItem
                name="sqlParameters.maxValue"
                initialValue={value.assertion?.sqlAssertion?.parameters?.maxValue?.value}
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
                <InputNumber
                    value={value.assertion?.sqlAssertion?.parameters?.maxValue?.value}
                    onChange={updateMaxValue}
                    disabled={disabled}
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
