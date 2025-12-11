/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

export const IconWrapper = styled.div<{ size: string; rotate?: string }>`
    position: relative;

    display: flex;
    align-items: center;
    justify-content: center;

    width: ${({ size }) => size};
    height: ${({ size }) => size};

    & svg {
        width: 100%;
        height: 100%;

        transform: ${({ rotate }) => rotate};
    }
`;
