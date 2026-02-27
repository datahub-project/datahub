import styled, { keyframes } from 'styled-components';

import { AlignItemsOptions, JustifyContentOptions } from '@components/components/Loader/types';

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

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

export const StyledSpinner = styled.span<{ $height: number }>`
    font-size: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    width: ${(props) => props.$height}px;
    position: absolute;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    animation: ${spin} 1s linear infinite;
    color: ${({ theme }) => theme.styles['primary-color']};
`;

export const LoaderBackRing = styled.span<{ $height: number; $ringWidth: number }>`
    width: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    border-radius: 50%;
`;
