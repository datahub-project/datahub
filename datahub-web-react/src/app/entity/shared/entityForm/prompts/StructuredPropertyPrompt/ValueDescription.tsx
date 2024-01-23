import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';
import { useEntityFormContext } from '../../EntityFormContext';

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
    const { displayBulkPromptStyles } = useEntityFormContext();

    return (
        <>
            <DescriptionSeparator>-</DescriptionSeparator>
            <DescriptionText displayBulkPromptStyles={displayBulkPromptStyles}>{description}</DescriptionText>
        </>
    );
}
