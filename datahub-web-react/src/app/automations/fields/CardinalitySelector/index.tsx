import React from 'react';
import styled from 'styled-components';

import type { ComponentBaseProps } from '@app/automations/types';
import { CardinalityType, CARDINALITY_TYPE_OPTIONS } from '@app/automations/constants';
import { Checkbox } from 'antd';
import { Tooltip } from '@components';
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
    cursor: pointer;
    & label {
        font-size: 14px;
    }
`;

export type CardinalitySelectorStateType = {
    cardinality: CardinalityType;
};

export const CardinalitySelector = ({ state, passStateToParent, props }: ComponentBaseProps) => {
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
                <Checkbox
                    id={multipleCardinality.name}
                    name="applyType"
                    value={multipleCardinality.name}
                    checked={checked}
                    onChange={handleRadioChange}
                />
                <label htmlFor={multipleCardinality.name} aria-checked={checked}>
                    {multipleCardinality.displayName}
                </label>
                <Tooltip showArrow={false} placement="right" title={props.description}>
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
        </RadiosContainer>
    );
};
