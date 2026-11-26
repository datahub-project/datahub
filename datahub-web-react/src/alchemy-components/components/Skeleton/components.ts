import styled, { css, keyframes } from 'styled-components';

import { SKELETON_LINE_HEIGHT } from '@components/components/Skeleton/constants';

const shimmer = keyframes`
    from { background-position: 100% 50%; }
    to { background-position: 0 50%; }
`;

const skeletonFill = css<{ $active: boolean }>`
    background: ${(props) => props.theme.colors.bgSkeleton};

    ${(props) =>
        props.$active &&
        css`
            background: linear-gradient(
                90deg,
                ${props.theme.colors.bgSkeleton} 25%,
                ${props.theme.colors.bgSkeletonShimmer} 37%,
                ${props.theme.colors.bgSkeleton} 63%
            );
            background-size: 400% 100%;
            animation: ${shimmer} 1.4s ease infinite;
        `}
`;

export const SkeletonContainer = styled.div`
    display: block;
    width: 100%;
`;

export const SkeletonTitle = styled.h3<{ $active: boolean; $width?: string }>`
    ${skeletonFill}
    width: ${(props) => props.$width ?? '100%'};
    height: ${SKELETON_LINE_HEIGHT}px;
    margin: 0;
    border-radius: 4px;
`;

export const SkeletonParagraph = styled.ul`
    margin: 0;
    padding: 0;
    list-style: none;

    ${SkeletonTitle} + & {
        margin-top: 24px;
    }
`;

export const SkeletonRow = styled.li<{ $active: boolean; $width?: string }>`
    ${skeletonFill}
    width: ${(props) => props.$width ?? '100%'};
    height: ${SKELETON_LINE_HEIGHT}px;
    border-radius: 4px;

    & + & {
        margin-top: ${SKELETON_LINE_HEIGHT}px;
    }
`;

export const SkeletonElement = styled.span<{
    $active: boolean;
    $width: string;
    $height: string;
    $borderRadius: string;
}>`
    ${skeletonFill}
    display: inline-block;
    vertical-align: top;
    width: ${(props) => props.$width};
    height: ${(props) => props.$height};
    border-radius: ${(props) => props.$borderRadius};
`;
