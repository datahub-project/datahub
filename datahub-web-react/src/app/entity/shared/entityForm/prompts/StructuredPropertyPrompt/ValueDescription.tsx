import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';

const DescriptionText = styled.span`
    color: ${ANTD_GRAY_V2[8]};
`;

const DescriptionSeparator = styled.span`
    margin: 0 8px;
`;

interface Props {
    description: string;
}

export default function ValueDescription({ description }: Props) {
    return (
        <>
            <DescriptionSeparator>-</DescriptionSeparator>
            <DescriptionText>{description}</DescriptionText>
        </>
    );
}
