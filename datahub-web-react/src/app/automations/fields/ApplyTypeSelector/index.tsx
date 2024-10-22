import React from 'react';
import styled from 'styled-components';

import type { ComponentBaseProps } from '@app/automations/types';
import { AutomationApplyType, APPLICATION_TYPE_OPTIONS } from '@app/automations/constants';
import { Checkbox, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';

const RadiosContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
`;

const RadioWrapper = styled.div`
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    justify-content: flex-start;
    gap: 10px;
    padding-right: 14px;

    & input[type='checkbox'] {
        flex: 0;
        margin-top: 5px;
    }

    & label {
        font-size: 14px;
    }

    &:hover,
    & input[type='checkbox']:hover,
    & label:hover {
        cursor: pointer;
    }
`;

// State Type (ensures the state is correctly applied across templates)
export type ApplyTypeSelectorStateType = {
    applyType: AutomationApplyType;
};

export const ApplyTypeSelector = ({ state, passStateToParent, props }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { applyType } = state as ApplyTypeSelectorStateType;
    const handleRadioChange = (event) => {
        passStateToParent({
            applyType: event.target.checked ? APPLICATION_TYPE_OPTIONS[0].key : APPLICATION_TYPE_OPTIONS[1].key,
        });
    };
    const propose = APPLICATION_TYPE_OPTIONS[0];
    const checked = propose.key === applyType;
    return (
        <RadiosContainer>
            <RadioWrapper>
                <Checkbox
                    id={propose.name}
                    name="applyType"
                    value={propose.name}
                    checked={checked}
                    onChange={handleRadioChange}
                />
                <label htmlFor={propose.name} aria-checked={checked}>
                    {propose.displayName}
                </label>
                <Tooltip showArrow={false} placement="right" title={props.description}>
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
        </RadiosContainer>
    );
};
