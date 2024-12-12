import React from 'react';
import { Pill } from '@components';

type PillMoMProps = {
    value?: number | null;
};

export default function MonthOverMonthPill({ value }: PillMoMProps) {
    if (value === undefined || value === null) return null;

    if (value > 0) return <Pill label={`${value}% MoM`} leftIcon="TrendingUp" size="sm" colorScheme="green" />;
    if (value < 0) return <Pill label={`${value}% MoM`} leftIcon="TrendingDown" size="sm" colorScheme="red" />;
    return <Pill label={`${value}% MoM`} size="sm" colorScheme="gray" />;
}
