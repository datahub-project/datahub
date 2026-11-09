import { HTMLAttributes } from 'react';

export type SkeletonWidth = number | string;

export type SkeletonSizeOptions = 'sm' | 'md' | 'lg';

export type SkeletonButtonShape = 'default' | 'square' | 'round' | 'circle';

export type SkeletonAvatarShape = 'circle' | 'square';

export type SkeletonTitleProps = {
    width?: SkeletonWidth;
};

export type SkeletonParagraphProps = {
    rows?: number;
    /** A single width applies to the last row; an array sets each row's width in order. */
    width?: SkeletonWidth | SkeletonWidth[];
};

export type SkeletonProps = Omit<HTMLAttributes<HTMLDivElement>, 'title'> & {
    /** Show the shimmer animation. */
    active?: boolean;
    /** Show a title bar above the paragraph; pass an object to set its width. */
    title?: boolean | SkeletonTitleProps;
    /** Show paragraph rows; pass an object to set the row count and widths. */
    paragraph?: boolean | SkeletonParagraphProps;
};

type SkeletonElementBaseProps = HTMLAttributes<HTMLSpanElement> & {
    /** Show the shimmer animation. */
    active?: boolean;
    /** Preset size, or a height in px. */
    size?: SkeletonSizeOptions | number;
    /** Overrides the width derived from `size` and `block`. */
    width?: SkeletonWidth;
    /** Overrides the height derived from `size`. */
    height?: SkeletonWidth;
};

export type SkeletonButtonProps = SkeletonElementBaseProps & {
    shape?: SkeletonButtonShape;
    /** Stretch to the full width of the parent. */
    block?: boolean;
};

export type SkeletonInputProps = SkeletonElementBaseProps & {
    /** Stretch to the full width of the parent. */
    block?: boolean;
};

export type SkeletonAvatarProps = SkeletonElementBaseProps & {
    shape?: SkeletonAvatarShape;
};
