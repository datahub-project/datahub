import { Pill } from '@components';
import React, { useMemo } from 'react';

import { BadgeProps } from './types';
import { formatBadgeValue } from './utils';
import { BadgeContainer } from './components';

export const badgeDefault: BadgeProps = {
    count: 0,
    overflowCount: 99,
    showZero: false,
};

export function Badge({
    count = badgeDefault.count,
    overflowCount = badgeDefault.overflowCount,
    showZero = badgeDefault.showZero,
    ...props
}: BadgeProps) {
    const label = useMemo(() => formatBadgeValue(count, overflowCount), [count, overflowCount]);

    if (!showZero && count === 0) return null;

    return (
        <BadgeContainer title={`${count}`}>
            <Pill label={label} {...props} />
        </BadgeContainer>
    );
}
