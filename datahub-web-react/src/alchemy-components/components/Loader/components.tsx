import { CircleNotch } from '@phosphor-icons/react';
import styled, { keyframes } from 'styled-components';

import { AlignItemsOptions, JustifyContentOptions } from '@components/components/Loader/types';

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

export const LoaderWrapper = styled.div<{
    $justifyContent: JustifyContentOptions;
    $alignItems: AlignItemsOptions;
    $padding?: number;
}>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    align-items: ${(props) => props.$alignItems};
    width: 100%;

    ${(props) => props.$padding !== undefined && `padding: ${props.$padding}px;`}
`;

export const StyledSpinner = styled(CircleNotch)<{ $height: number }>`
    width: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    animation: ${spin} 1s linear infinite;
    color: ${({ theme }) => theme?.colors?.iconBrand};
`;
