import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import React from 'react';
import styled, { keyframes } from 'styled-components';

const LoadingWrapper = styled.div<{
    $marginTop?: number;
    $width?: number;
    $justifyContent: 'center' | 'flex-start';
    $alignItems: 'center' | 'flex-start' | 'none';
}>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    align-items: ${(props) => props.$alignItems};
    margin-top: ${(props) => (typeof props.$marginTop === 'number' ? `${props.$marginTop}px` : '25%')};
    width: ${({ $width }) => ($width !== undefined ? `${$width}px` : '100%')};
`;

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

const StyledLoading = styled(CircleNotch)<{ $height: number }>`
    width: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    animation: ${spin} 1s linear infinite;
`;

interface Props {
    height?: number;
    width?: number;
    marginTop?: number;
    justifyContent?: 'center' | 'flex-start';
    alignItems?: 'center' | 'flex-start' | 'none';
}

export default function Loading({
    height = 32,
    width,
    justifyContent = 'center',
    alignItems = 'none',
    marginTop,
}: Props) {
    return (
        <LoadingWrapper $marginTop={marginTop} $width={width} $justifyContent={justifyContent} $alignItems={alignItems}>
            <StyledLoading $height={height} />
        </LoadingWrapper>
    );
}
