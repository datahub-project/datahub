import { Icon, typography } from '@components';
import React from 'react';

import { FontSizeOptions } from '@components/theme/config';

type Props = {
    size?: number;
};

export default function LogicalPlatformDefaultIcon({ size }: Props) {
    const sizeStr = Object.entries(typography.fontSizes).find(([_k, v]) => v === `${size}px`)?.[0] || '4xl';

    return <Icon icon="IntersectSquare" source="phosphor" size={sizeStr as FontSizeOptions} color="gray" />;
}
