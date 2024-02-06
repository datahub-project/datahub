import React from 'react';
import styled from 'styled-components';
import { pluralize } from '../../../../../../shared/textUtil';
import { ANTD_GRAY_V2 } from '../../../../constants';

const OptionalPromptsWrapper = styled.div`
    color: ${ANTD_GRAY_V2[8]};
    margin-top: 4px;
`;

interface Props {
    numRemaining: number;
}

export default function OptionalPromptsRemaining({ numRemaining }: Props) {
    if (numRemaining <= 0) return null;

    return (
        <OptionalPromptsWrapper>
            {numRemaining} additional {pluralize(numRemaining, 'question', 's')} remaining
        </OptionalPromptsWrapper>
    );
}
