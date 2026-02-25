import React from 'react';
import styled from 'styled-components';

const StyledValue = styled.div`
    font-family: Manrope;
    font-size: 14px;
    font-style: normal;
    font-weight: 400;
    line-height: 22px;
    color: ${(props) => props.theme.colors.text};
`;

const StyledDescription = styled.div`
    font-family: Manrope;
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    value: string | number | null;
    description?: string | null;
}

export default function DropdownLabel({ value, description }: Props) {
    return (
        <>
            <StyledValue>{value}</StyledValue>
            <StyledDescription>{description}</StyledDescription>
        </>
    );
}
