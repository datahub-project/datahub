import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';

const DescriptionText = styled.span<{ displayBulkPromptStyles: boolean }>`
    color: ${ANTD_GRAY_V2[8]};
    ${(props) => props.displayBulkPromptStyles && `color: ${ANTD_GRAY_V2[5]};`}
`;

const DescriptionSeparator = styled.span`
    margin: 0 8px;
`;

interface Props {
    description: string;
}

export default function ValueDescription({ description }: Props) {
    const {
        prompt: { displayBulkPromptStyles },
    } = useEntityFormContext();

    return (
        <>
            <DescriptionSeparator>-</DescriptionSeparator>
            <DescriptionText displayBulkPromptStyles={displayBulkPromptStyles}>{description}</DescriptionText>
        </>
    );
}
