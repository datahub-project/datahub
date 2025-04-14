import { Form, Select } from 'antd';
import React, { useEffect, useRef } from 'react';
import { AssertionFormTitleAndTooltip } from './AssertionFormTitleAndTooltip';

type Props = {
    sensitivity?: number;
    disabled?: boolean;
    onChange: (value: number) => void;
};

const DEFAULT_SENSITIVITY = 5; // Medium

export const InferenceSensitivityAdjuster = (props: Props) => {
    const { sensitivity, disabled, onChange } = props;

    const initialValueRef = useRef(sensitivity || DEFAULT_SENSITIVITY);
    useEffect(() => {
        onChange(initialValueRef.current);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    /* High medium low dropdown, defaults to medium */
    return (
        <Form.Item
            label={
                <AssertionFormTitleAndTooltip
                    formTitle="Sensitivity"
                    tooltipTitle="Sensitivity"
                    tooltipDescription="Set how tight the predicted assertion bounds will be. High sensitivity will result in a tighter fit and potentially more false positive alerts, while low sensitivity will be lower and can result in fewer alerts."
                />
            }
            name="sensitivity"
            initialValue={initialValueRef.current}
            labelCol={{ span: 24 }}
            wrapperCol={{ span: 24 }}
            style={{ marginBottom: 8 }}
        >
            <Select
                options={[
                    { label: 'High', value: 10 },
                    { label: 'Medium', value: 5 },
                    { label: 'Low', value: 1 },
                ]}
                disabled={disabled}
                style={{ width: 140 }}
                onChange={onChange}
            />
        </Form.Item>
    );
};
