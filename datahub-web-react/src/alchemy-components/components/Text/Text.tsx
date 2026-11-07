import React, { forwardRef } from 'react';

import { Div, P, Pre, Span } from '@components/components/Text/components';
import { TextProps } from '@components/components/Text/types';

export const textDefaults: TextProps = {
    type: 'p',
    color: 'inherit',
    size: 'md',
    weight: 'normal',
};

// Forwards its ref so Text can be a Tooltip/Popover child, which attach to the DOM node through a ref.
export const Text = forwardRef<HTMLElement, TextProps>(
    (
        {
            type = textDefaults.type,
            color = textDefaults.color,
            size = textDefaults.size,
            weight = textDefaults.weight,
            children,
            ...props
        },
        ref,
    ) => {
        const sharedProps = { size, color, weight, ...props };

        switch (type) {
            case 'p':
                return (
                    <P ref={ref as React.Ref<HTMLParagraphElement>} {...sharedProps}>
                        {children}
                    </P>
                );
            case 'div':
                return (
                    <Div ref={ref as React.Ref<HTMLDivElement>} {...sharedProps}>
                        {children}
                    </Div>
                );
            case 'span':
                return (
                    <Span ref={ref as React.Ref<HTMLSpanElement>} {...sharedProps}>
                        {children}
                    </Span>
                );
            case 'pre':
                return (
                    <Pre ref={ref as React.Ref<HTMLPreElement>} {...sharedProps}>
                        {children}
                    </Pre>
                );
            default:
                return (
                    <P ref={ref as React.Ref<HTMLParagraphElement>} {...sharedProps}>
                        {children}
                    </P>
                );
        }
    },
);

Text.displayName = 'Text';
