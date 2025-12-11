/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import ColorHash from 'color-hash';
import React from 'react';
import styled, { css } from 'styled-components';

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
