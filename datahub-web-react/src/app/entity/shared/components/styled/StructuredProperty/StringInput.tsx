import { Input } from 'antd';
import React, { ChangeEvent } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';
import { PropertyCardinality } from '../../../../../../types.generated';
import MultipleOpenEndedInput from './MultipleOpenEndedInput';

const StyledInput = styled(Input)`
    width: 75%;
    min-width: 350px;
    max-width: 500px;
    border: 1px solid ${ANTD_GRAY_V2[6]};
`;

interface Props {
    selectedValues: any[];
    cardinality?: PropertyCardinality | null;
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function StringInput({ selectedValues, cardinality, updateSelectedValues }: Props) {
    function updateInput(event: ChangeEvent<HTMLInputElement>) {
        updateSelectedValues([event.target.value]);
    }

    if (cardinality === PropertyCardinality.Multiple) {
        return <MultipleOpenEndedInput selectedValues={selectedValues} updateSelectedValues={updateSelectedValues} />;
    }

    return (
        <StyledInput
            type="text"
            value={selectedValues[0] || ''}
            onChange={updateInput}
            data-testid="structured-property-string-value-input"
        />
    );
}
