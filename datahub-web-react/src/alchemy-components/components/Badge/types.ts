import { HTMLAttributes } from 'react';

import { PillProps } from '@components/components/Pills/types';

export interface BadgeProps extends Omit<PillProps, 'label'>, Omit<HTMLAttributes<HTMLElement>, 'color'> {
    count: number;
    overflowCount?: number;
    showZero?: boolean;
}
