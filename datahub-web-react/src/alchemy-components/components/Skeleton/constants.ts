import { SkeletonSizeOptions } from '@components/components/Skeleton/types';

// Heights match the antd Skeleton elements these replace, so swapping them in keeps layouts unchanged.
export const SKELETON_ELEMENT_HEIGHTS: Record<SkeletonSizeOptions, number> = {
    sm: 24,
    md: 32,
    lg: 40,
};

export const SKELETON_LINE_HEIGHT = 16;
export const SKELETON_DEFAULT_TITLE_WIDTH = '38%';
export const SKELETON_DEFAULT_LAST_ROW_WIDTH = '61%';
