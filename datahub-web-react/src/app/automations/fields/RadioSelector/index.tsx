import { Radio } from 'antd';
import React from 'react';

import { CheckboxGroup, CustomCheckboxLabel } from '@app/automations/sharedComponents';
import type { ComponentBaseProps } from '@app/automations/types';

// Custom Checkbox w/ Label component
const RadioLabel = ({ label, description }: any) => (
    <CustomCheckboxLabel>
        <strong>{label}</strong>
        <p>{description}</p>
    </CustomCheckboxLabel>
);

// Generic RadioSelector Component
export const RadioSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    const { options, fieldName } = props;

    // Access the selected value based on the selectedKey
    const selectedValue = state[fieldName];

    return (
        <>
            <CheckboxGroup>
                <Radio.Group
                    options={options.map((opt) => ({
                        label: <RadioLabel label={opt.name} description={opt.description} />,
                        value: opt.key,
                    }))}
                    value={selectedValue}
                    onChange={(evt) => passStateToParent({ [fieldName]: evt.target.value })}
                />
            </CheckboxGroup>
        </>
    );
};
