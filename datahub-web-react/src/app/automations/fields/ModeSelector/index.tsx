import { Radio } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { MODE_OPTIONS, ModeTypes } from '@app/automations/fields/ModeSelector/constants';
import type { ComponentBaseProps } from '@app/automations/types';

const RadioGroupWrapper = styled.div`
    &&& .ant-radio-wrapper {
        display: flex;
        align-items: center;
        margin-bottom: 8px;

        & > span {
            display: block;
            line-height: 16px;
            top: 0;
        }
    }
`;

const RadioOption = styled.div`
    font-weight: normal;
`;

// State Type (ensures the state is correctly applied across templates)
export type ModeSelectorStateType = {
    mode?: ModeTypes;
};

// Custom Checkbox w/ Label component
const CheckboxLabel = ({ label, description }: any) => (
    <RadioOption>
        <strong>{label}</strong> - {description}
    </RadioOption>
);

// Component
export const ModeSelector = ({ state, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { mode } = state as ModeSelectorStateType;

    // Return the radios for mode
    return (
        <RadioGroupWrapper>
            <Radio.Group
                options={MODE_OPTIONS.map((opt) => ({
                    label: <CheckboxLabel label={opt.name} description={opt.description} />,
                    value: opt.key,
                }))}
                defaultValue={mode}
                onChange={(evt) => passStateToParent({ mode: evt.target.value })}
            />
        </RadioGroupWrapper>
    );
};
