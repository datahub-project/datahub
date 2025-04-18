import { Pill } from '@components';
import React from 'react';

type PillMoMProps = {
    value?: number | null;
};

// FYI: The month over month functionality is temporary disabled
// see `../utils->addMonthOverMonthValue`
const IS_MOM_PILL_DISABLED = true;

export default function MonthOverMonthPill({ value }: PillMoMProps) {
    if (IS_MOM_PILL_DISABLED) return null;

    if (value === undefined || value === null) return null;

    if (value > 0) return <Pill label={`${value}% MoM`} leftIcon="TrendingUp" size="sm" color="green" />;
    if (value < 0) return <Pill label={`${value}% MoM`} leftIcon="TrendingDown" size="sm" color="red" />;
    return <Pill label={`${value}% MoM`} size="sm" color="gray" />;
}
