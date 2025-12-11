/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
