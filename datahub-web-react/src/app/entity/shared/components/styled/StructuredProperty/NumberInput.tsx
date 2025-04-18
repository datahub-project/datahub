import { Input } from 'antd';
import React, { ChangeEvent } from 'react';
import styled from 'styled-components';
import { PropertyCardinality } from '@src/types.generated';
import { ANTD_GRAY_V2 } from '../../../constants';
import MultipleOpenEndedInput from './MultipleOpenEndedInput';

const StyledInput = styled(Input)`
    border: 1px solid ${ANTD_GRAY_V2[6]};
    width: 250px;
`;

interface Props {
    selectedValues: any[];
    cardinality?: PropertyCardinality | null;
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function NumberInput({ selectedValues, cardinality, updateSelectedValues }: Props) {
    function updateInput(event: ChangeEvent<HTMLInputElement>) {
        const number = Number(event.target.value);
        updateSelectedValues([number]);
    }

    function updateMultipleValues(values: string[] | number[]) {
        const numbers = values.map((v) => Number(v));
        updateSelectedValues(numbers);
    }

    if (cardinality === PropertyCardinality.Multiple) {
        return (
            <MultipleOpenEndedInput
                selectedValues={selectedValues}
                updateSelectedValues={updateMultipleValues}
                inputType="number"
            />
        );
    }

    return (
        <StyledInput
            type="number"
            value={selectedValues[0] !== undefined ? selectedValues[0] : null}
            onChange={updateInput}
        />
    );
}
