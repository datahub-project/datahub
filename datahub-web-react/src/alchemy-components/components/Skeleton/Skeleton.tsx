import React from 'react';

import { SkeletonAvatar, SkeletonButton, SkeletonInput } from '@components/components/Skeleton/SkeletonElements';
import {
    SkeletonContainer,
    SkeletonParagraph,
    SkeletonRow,
    SkeletonTitle,
} from '@components/components/Skeleton/components';
import { SkeletonProps } from '@components/components/Skeleton/types';
import { getParagraphRowWidths, getTitleWidth } from '@components/components/Skeleton/utils';

export const skeletonDefault: SkeletonProps = {
    active: true,
    title: true,
    paragraph: true,
};

function SkeletonBase({
    active = skeletonDefault.active,
    title = skeletonDefault.title,
    paragraph = skeletonDefault.paragraph,
    ...props
}: SkeletonProps) {
    const isActive = !!active;
    const hasTitle = !!title;
    const hasParagraph = !!paragraph;

    return (
        <SkeletonContainer aria-busy="true" {...props}>
            {hasTitle && <SkeletonTitle $active={isActive} $width={getTitleWidth(title, hasParagraph)} />}
            {hasParagraph && (
                <SkeletonParagraph>
                    {getParagraphRowWidths(paragraph, hasTitle).map((width, index) => (
                        // eslint-disable-next-line react/no-array-index-key -- rows are static placeholders
                        <SkeletonRow key={index} $active={isActive} $width={width} />
                    ))}
                </SkeletonParagraph>
            )}
        </SkeletonContainer>
    );
}

export const Skeleton = Object.assign(SkeletonBase, {
    Button: SkeletonButton,
    Input: SkeletonInput,
    Avatar: SkeletonAvatar,
});
