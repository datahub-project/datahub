import { Input } from '@components';
import React from 'react';

import MultipleOpenEndedInput from '@app/entity/shared/components/styled/StructuredProperty/MultipleOpenEndedInput';
import { PropertyCardinality } from '@src/types.generated';

// Programmatic discriminator for MultipleOpenEndedInput; not user-visible text.
const NUMBER_INPUT_TYPE = 'number';

interface Props {
    selectedValues: (string | number | null)[];
    cardinality?: PropertyCardinality | null;
    updateSelectedValues: (values: (string | number | null)[]) => void;
}

export default function NumberInput({ selectedValues, cardinality, updateSelectedValues }: Props) {
    function updateInput(value: string) {
        const number = Number(value);
        updateSelectedValues([number]);
    }

    function updateMultipleValues(values: (string | number | null)[]) {
        const numbers = values.map((v) => Number(v));
        updateSelectedValues(numbers);
    }

    if (cardinality === PropertyCardinality.Multiple) {
        return (
            <MultipleOpenEndedInput
                selectedValues={selectedValues}
                updateSelectedValues={updateMultipleValues}
                inputType={NUMBER_INPUT_TYPE}
            />
        );
    }

    return (
        <Input
            type="number"
            value={selectedValues[0] !== undefined ? String(selectedValues[0]) : ''}
            setValue={updateInput}
        />
    );
}
