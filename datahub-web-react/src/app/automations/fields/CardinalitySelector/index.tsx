import React from 'react';
import styled from 'styled-components';

import type { ComponentBaseProps } from '@app/automations/types';
import { CardinalityType, CARDINALITY_TYPE_OPTIONS } from '@app/automations/constants';
import { Checkbox } from 'antd';

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
    const handleRadioChange = (event) => {
        passStateToParent({
            cardinality: event.target.checked ? CARDINALITY_TYPE_OPTIONS[1].key : CARDINALITY_TYPE_OPTIONS[0].key,
        });
    };

    const multipleCardinality = CARDINALITY_TYPE_OPTIONS[1];
    const checked = cardinality === multipleCardinality.key;
    return (
        <RadiosContainer>
            <RadioWrapper>
                <div>
                    <Checkbox
                        id={multipleCardinality.name}
                        name="applyType"
                        value={multipleCardinality.name}
                        checked={checked}
                        onChange={handleRadioChange}
                    />
                </div>
                <div>
                    <label htmlFor={multipleCardinality.name} aria-checked={checked}>
                        {multipleCardinality.displayName}
                    </label>
                </div>
            </RadioWrapper>
        </RadiosContainer>
    );
};
