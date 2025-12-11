/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Input } from 'antd';
import React, { ChangeEvent } from 'react';
import styled from 'styled-components';

import MultipleOpenEndedInput from '@app/entity/shared/components/styled/StructuredProperty/MultipleOpenEndedInput';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';

import { PropertyCardinality } from '@types';

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
