import React from 'react';

import { H1, H2, H3, H4, H5, H6 } from '@components/components/Heading/components';
import { HeadingProps, HeadingPropsDefaults } from '@components/components/Heading/types';

export const headingDefaults: HeadingPropsDefaults = {
    type: 'h1',
    color: 'inherit',
    size: '2xl',
    weight: 'medium',
};

export const Heading = ({
    type = headingDefaults.type,
    size = headingDefaults.size,
    color = headingDefaults.color,
    weight = headingDefaults.weight,
    colorLevel,
    children,
}: HeadingProps) => {
    const sharedProps = { size, color, colorLevel, weight };

    switch (type) {
        case 'h1':
            return <H1 {...sharedProps}>{children}</H1>;
        case 'h2':
            return <H2 {...sharedProps}>{children}</H2>;
        case 'h3':
            return <H3 {...sharedProps}>{children}</H3>;
        case 'h4':
            return <H4 {...sharedProps}>{children}</H4>;
        case 'h5':
            return <H5 {...sharedProps}>{children}</H5>;
        case 'h6':
            return <H6 {...sharedProps}>{children}</H6>;
        default:
            return <H1 {...sharedProps}>{children}</H1>;
    }
};
