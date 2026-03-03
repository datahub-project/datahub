import React from 'react';

import { Div, P, Pre, Span } from '@components/components/Text/components';
import { TextProps } from '@components/components/Text/types';

export const textDefaults: TextProps = {
    type: 'p',
    color: 'inherit',
    size: 'md',
    weight: 'normal',
};

export const Text = ({
    type = textDefaults.type,
    color = textDefaults.color,
    size = textDefaults.size,
    weight = textDefaults.weight,
    children,
    ...props
}: TextProps) => {
    const sharedProps = { size, color, weight, ...props };

    switch (type) {
        case 'p':
            return <P {...sharedProps}>{children}</P>;
        case 'div':
            return <Div {...sharedProps}>{children}</Div>;
        case 'span':
            return <Span {...sharedProps}>{children}</Span>;
        case 'pre':
            return <Pre {...sharedProps}>{children}</Pre>;
        default:
            return <P {...sharedProps}>{children}</P>;
    }
};
