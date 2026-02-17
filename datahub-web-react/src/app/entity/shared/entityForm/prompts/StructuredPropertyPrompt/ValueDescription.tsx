import React from 'react';
import styled from 'styled-components';


const DescriptionText = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
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
