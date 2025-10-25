import { Form, InputNumber } from 'antd';
import React from 'react';

import { AssertionFormTitleAndTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/AssertionFormTitleAndTooltip';

type Props = {
    trainingDataLookbackWindowDays?: number;
    disabled?: boolean;
    onChange: (value: number) => void;
};

export const DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS = 60;

export const LookBackWindowAdjuster = (props: Props) => {
    const { trainingDataLookbackWindowDays, disabled, onChange } = props;
    return (
        <Form.Item
            label={
                <AssertionFormTitleAndTooltip
                    formTitle="Maximum training data days"
                    formSubtitle="This does not affect the minimum number of days required for training."
                    tooltipTitle="Maximum training data days"
                    tooltipDescription="Number of days of historical data to use for training the smart assertion model. More days provide better seasonal detection but can be more susceptible to outliers."
                />
            }
            name="trainingDataLookbackWindowDays"
            initialValue={trainingDataLookbackWindowDays || DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS}
            labelCol={{ span: 24 }}
            wrapperCol={{ span: 24 }}
            style={{ marginBottom: 8 }}
            required={false} // to hide the asterisk
            rules={[
                { required: true, message: 'Please input training data lookback window days' },
                { type: 'number', min: 7, message: 'Days must be 7 or more, to ensure enough data for training.' },
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
