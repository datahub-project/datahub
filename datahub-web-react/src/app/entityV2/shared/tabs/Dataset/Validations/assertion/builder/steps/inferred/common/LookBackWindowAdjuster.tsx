import { InputNumber, Form } from 'antd';
import React from 'react';
import { AssertionFormTitleAndTooltip } from './AssertionFormTitleAndTooltip';

type Props = {
    trainingDataLookbackWindowDays?: number;
    disabled?: boolean;
    onChange: (value: number) => void;
};

export const LookBackWindowAdjuster = (props: Props) => {
    const { trainingDataLookbackWindowDays, disabled, onChange } = props;
    return (
        <Form.Item
            label={
                <AssertionFormTitleAndTooltip
                    formTitle="Training data lookback window days"
                    tooltipTitle="Training data lookback window days"
                    tooltipDescription="Set the number of days to look back for training data when inferring assertion bounds."
                />
            }
            name="trainingDataLookbackWindowDays"
            initialValue={trainingDataLookbackWindowDays || 60}
            labelCol={{ span: 24 }}
            wrapperCol={{ span: 24 }}
            style={{ marginBottom: 8 }}
            required={false} // to hide the asterisk
            rules={[
                { required: true, message: 'Please input training data lookback window days' },
                { type: 'number', min: 1, message: 'Days must be greater than 0' },
            ]}
        >
            <InputNumber
                min={1}
                onChange={(value) => {
                    const numValue = typeof value === 'string' ? parseFloat(value) : value;
                    if (numValue && numValue > 0) {
                        onChange(numValue);
                    }
                }}
                disabled={disabled}
            />
        </Form.Item>
    );
};
