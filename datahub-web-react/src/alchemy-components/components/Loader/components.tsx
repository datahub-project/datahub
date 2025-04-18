import { LoadingOutlined } from '@ant-design/icons';
import { colors } from '@src/alchemy-components/theme';
import styled from 'styled-components';
import { AlignItemsOptions, JustifyContentOptions } from './types';

export const LoaderWrapper = styled.div<{
    $marginTop?: number;
    $justifyContent: JustifyContentOptions;
    $alignItems: AlignItemsOptions;
}>`
    display: flex;
    justify-content: ${(props) => props.$justifyContent};
    align-items: ${(props) => props.$alignItems};
    margin: auto;
    width: 100%;
    position: relative;
`;

export const StyledLoadingOutlined = styled(LoadingOutlined)<{ $height: number }>`
    font-size: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    position: absolute;

    svg {
        fill: ${colors.violet[500]};
    }
`;

export const LoaderBackRing = styled.span<{ $height: number; $ringWidth: number }>`
    width: ${(props) => props.$height}px;
    height: ${(props) => props.$height}px;
    border: ${(props) => props.$ringWidth}px solid ${colors.gray[100]};
    border-radius: 50%;
`;
