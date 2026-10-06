import { Input } from '@components';
import React from 'react';

import MultipleOpenEndedInput from '@app/entity/shared/components/styled/StructuredProperty/MultipleOpenEndedInput';

import { PropertyCardinality } from '@types';

interface Props {
    selectedValues: (string | number | null)[];
    cardinality?: PropertyCardinality | null;
    updateSelectedValues: (values: (string | number | null)[]) => void;
}

export default function StringInput({ selectedValues, cardinality, updateSelectedValues }: Props) {
    if (cardinality === PropertyCardinality.Multiple) {
        return <MultipleOpenEndedInput selectedValues={selectedValues} updateSelectedValues={updateSelectedValues} />;
    }

    return (
        <Input
            type="text"
            value={selectedValues[0] || ''}
            setValue={(value) => updateSelectedValues([value])}
            inputTestId="structured-property-string-value-input"
        />
    );
}
