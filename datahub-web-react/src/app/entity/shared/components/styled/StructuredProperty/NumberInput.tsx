import { Input } from 'antd';
import React, { ChangeEvent } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';

const StyledInput = styled(Input)`
    border: 1px solid ${ANTD_GRAY_V2[6]};
    width: 250px;
`;

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function NumberInput({ selectedValues, updateSelectedValues }: Props) {
    function updateInput(event: ChangeEvent<HTMLInputElement>) {
        const number = Number(event.target.value);
        updateSelectedValues([number]);
    }

    return (
        <StyledInput
            type="number"
            value={selectedValues[0] !== undefined ? selectedValues[0] : null}
            onChange={updateInput}
        />
    );
}
