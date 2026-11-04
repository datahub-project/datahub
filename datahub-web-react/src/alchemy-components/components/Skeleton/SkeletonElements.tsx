import React from 'react';

import { SkeletonElement } from '@components/components/Skeleton/components';
import { SkeletonAvatarProps, SkeletonButtonProps, SkeletonInputProps } from '@components/components/Skeleton/types';
import { getElementHeight, toCssSize } from '@components/components/Skeleton/utils';

const ELEMENT_BORDER_RADIUS = '4px';

export function SkeletonButton({
    active = true,
    size = 'md',
    shape = 'default',
    block = false,
    width,
    height,
    ...props
}: SkeletonButtonProps) {
    const baseHeight = getElementHeight(size);
    const isEqualSided = shape === 'circle' || shape === 'square';
    const defaultWidth = isEqualSided ? baseHeight : baseHeight * 2;
    let borderRadius = ELEMENT_BORDER_RADIUS;
    if (shape === 'circle') borderRadius = '50%';
    if (shape === 'round') borderRadius = '999px';

    return (
        <SkeletonElement
            $active={active}
            $width={toCssSize(width) ?? (block ? '100%' : `${defaultWidth}px`)}
            $height={toCssSize(height) ?? `${baseHeight}px`}
            $borderRadius={borderRadius}
            {...props}
        />
    );
}

export function SkeletonInput({
    active = true,
    size = 'md',
    block = false,
    width,
    height,
    ...props
}: SkeletonInputProps) {
    const baseHeight = getElementHeight(size);

    return (
        <SkeletonElement
            $active={active}
            $width={toCssSize(width) ?? (block ? '100%' : `${baseHeight * 5}px`)}
            $height={toCssSize(height) ?? `${baseHeight}px`}
            $borderRadius={ELEMENT_BORDER_RADIUS}
            {...props}
        />
    );
}

export function SkeletonAvatar({
    active = true,
    size = 'md',
    shape = 'circle',
    width,
    height,
    ...props
}: SkeletonAvatarProps) {
    const baseHeight = getElementHeight(size);

    return (
        <SkeletonElement
            $active={active}
            $width={toCssSize(width) ?? `${baseHeight}px`}
            $height={toCssSize(height) ?? `${baseHeight}px`}
            $borderRadius={shape === 'circle' ? '50%' : ELEMENT_BORDER_RADIUS}
            {...props}
        />
    );
}
