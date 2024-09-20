import React from 'react';
import styled from 'styled-components';

import { Text } from '@components';
import type { ComponentBaseProps } from '@app/automations/types';
import { CardinalityType, CARDINALITY_TYPE_OPTIONS } from '@app/automations/constants';

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

export type CardinalitySelectorStateType = {
    cardinality: CardinalityType;
};

export const CardinalitySelector = ({ state, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { cardinality } = state as CardinalitySelectorStateType;

    return (
        <RadiosContainer>
            {CARDINALITY_TYPE_OPTIONS.map((option) => {
                const checked = cardinality === option.key;
                const handleRadioChange = () => passStateToParent({ cardinality: option.key as CardinalityType });

                return (
                    <RadioWrapper key={option.key} onClick={handleRadioChange}>
                        <input type="radio" id={option.name} name="cardinality" value={option.key} checked={checked} />
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
