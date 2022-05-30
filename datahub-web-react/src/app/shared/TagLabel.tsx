import React from 'react';
import styled, { css } from 'styled-components';
import ColorHash from 'color-hash';

type Props = {
    name: string;
    colorHash: string;
    color?: string | null;
};

const generateColor = new ColorHash({
    saturation: 0.9,
});

export const StyledDiv = styled.div<{ $color: any; $colorHash?: string }>`
    ${(props) =>
        props.$colorHash &&
        css`
            &:before {
                display: inline-block;
                content: '';
                width: 8px;
                height: 8px;
                background: ${props.$color === null || props.$color === undefined
                    ? generateColor.hex(props.$colorHash)
                    : props.$color};
                border-radius: 100em;
                margin-right: 5px;
            }
        `}
`;

export default function TagLabel({ name, colorHash, color }: Props) {
    return (
        <StyledDiv $colorHash={colorHash} $color={color}>
            {name}
        </StyledDiv>
    );
}
