import { HTMLAttributes } from 'react';
import { PillProps } from '../Pills/types';

export interface BadgeProps extends HTMLAttributes<HTMLElement>, Omit<PillProps, 'label'> {
    count: number;
    overflowCount?: number;
    showZero?: boolean;
}
