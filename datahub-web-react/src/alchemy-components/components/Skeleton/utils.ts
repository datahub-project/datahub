import {
    SKELETON_DEFAULT_LAST_ROW_WIDTH,
    SKELETON_DEFAULT_TITLE_WIDTH,
    SKELETON_ELEMENT_HEIGHTS,
} from '@components/components/Skeleton/constants';
import {
    SkeletonParagraphProps,
    SkeletonSizeOptions,
    SkeletonTitleProps,
    SkeletonWidth,
} from '@components/components/Skeleton/types';

export function toCssSize(value: SkeletonWidth | undefined): string | undefined {
    if (value === undefined) return undefined;
    return typeof value === 'number' ? `${value}px` : value;
}

export function getElementHeight(size: SkeletonSizeOptions | number = 'md'): number {
    return typeof size === 'number' ? size : SKELETON_ELEMENT_HEIGHTS[size];
}

export function getTitleWidth(
    title: boolean | SkeletonTitleProps | undefined,
    hasParagraph: boolean,
): string | undefined {
    if (typeof title === 'object' && title.width !== undefined) return toCssSize(title.width);
    return hasParagraph ? SKELETON_DEFAULT_TITLE_WIDTH : undefined;
}

/**
 * Resolves the width of every paragraph row. Mirrors antd's defaults: 3 rows under a title, 2 without,
 * and a shortened last row unless widths are given.
 */
export function getParagraphRowWidths(
    paragraph: boolean | SkeletonParagraphProps | undefined,
    hasTitle: boolean,
): (string | undefined)[] {
    const config = typeof paragraph === 'object' ? paragraph : {};
    const rows = config.rows ?? (hasTitle ? 3 : 2);
    const { width } = config;

    return Array.from({ length: rows }, (_, index) => {
        if (Array.isArray(width)) return toCssSize(width?.[index]);
        if (index !== rows - 1) return undefined;
        return width === undefined ? SKELETON_DEFAULT_LAST_ROW_WIDTH : toCssSize(width);
    });
}
