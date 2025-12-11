/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { AlignItemsOptions, JustifyContentOptions } from '@components/components/Loader/types';

import { colors } from '@src/alchemy-components/theme';

export const LoaderWrapper = styled.div<{
    $marginTop?: number;
    $justifyContent: JustifyContentOptions;
    $alignItems: AlignItemsOptions;
    $padding?: number;
}>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    align-items: ${(props) => props.$alignItems};
    margin: auto;
    width: 100%;
    position: relative;

    ${(props) => props.$padding !== undefined && `padding: ${props.$padding}px;`}
`;

export const StyledLoadingOutlined = styled(LoadingOutlined)<{ $height: number }>`
    font-size: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    position: absolute;

    svg {
        fill: ${({ theme }) => theme.styles['primary-color']};
    }
`;

export const LoaderBackRing = styled.span<{ $height: number; $ringWidth: number }>`
    width: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    border: ${(props) => props.$ringWidth}px solid ${colors.gray[100]};
    border-radius: 50%;
`;
