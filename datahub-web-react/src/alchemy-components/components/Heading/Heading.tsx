import React, { forwardRef } from 'react';

import { H1, H2, H3, H4, H5, H6 } from '@components/components/Heading/components';
import { HeadingProps, HeadingPropsDefaults } from '@components/components/Heading/types';

export const headingDefaults: HeadingPropsDefaults = {
    type: 'h1',
    color: 'inherit',
    size: '2xl',
    weight: 'medium',
};

const HEADING_ELEMENTS = { h1: H1, h2: H2, h3: H3, h4: H4, h5: H5, h6: H6 };

// Passes through HTML attributes (className, style, data-testid, ...) so styled(Heading) works,
// and forwards its ref so Heading can be a Tooltip/Popover child.
export const Heading = forwardRef<HTMLHeadingElement, HeadingProps>(
    (
        {
            type = headingDefaults.type,
            size = headingDefaults.size,
            color = headingDefaults.color,
            weight = headingDefaults.weight,
            colorLevel,
            children,
            ...props
        },
        ref,
    ) => {
        const Element = HEADING_ELEMENTS[type] || H1;

        return (
            <Element ref={ref} size={size} color={color} colorLevel={colorLevel} weight={weight} {...props}>
                {children}
            </Element>
        );
    },
);

Heading.displayName = 'Heading';
