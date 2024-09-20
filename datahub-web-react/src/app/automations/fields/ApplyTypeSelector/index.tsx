import React from 'react';
import styled from 'styled-components';

import { Text } from '@components';
import type { ComponentBaseProps } from '@app/automations/types';
import { AutomationApplyType, APPLICATION_TYPE_OPTIONS } from '@app/automations/constants';

const RadiosContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
`;

const RadioWrapper = styled.div`
    margin-bottom: 10px;
    display: flex;
    align-items: flex-start;
    justify-content: flex-start;
    gap: 10px;
    padding-right: 14px;

    & input[type='radio'] {
        flex: 0;
        margin-top: 5px;
    }

    & label {
        font-size: 14px;
    }

    &:hover,
    & input[type='radio']:hover,
    & label:hover {
        cursor: pointer;
    }
`;

// State Type (ensures the state is correctly applied across templates)
export type ApplyTypeSelectorStateType = {
    applyType: AutomationApplyType;
};

export const ApplyTypeSelector = ({ state, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { applyType } = state as ApplyTypeSelectorStateType;

    return (
        <RadiosContainer>
            {APPLICATION_TYPE_OPTIONS.map((option) => {
                const checked = applyType === option.key;
                const handleRadioChange = () => passStateToParent({ applyType: option.key as AutomationApplyType });

                return (
                    <RadioWrapper key={option.key} onClick={handleRadioChange}>
                        <input type="radio" id={option.name} name="applyType" value={option.key} checked={checked} />
                        <div>
                            <label htmlFor={option.name} aria-checked={checked}>
                                {option.name}
                            </label>
                            <Text size="sm" color="gray">
                                {option.description}
                            </Text>
                        </div>
                    </RadioWrapper>
                );
            })}
        </RadiosContainer>
    );
};
