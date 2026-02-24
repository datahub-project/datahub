import { Pill } from '@components';
import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';
import { ArrowUpRight } from '@phosphor-icons/react/dist/csr/ArrowUpRight';
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

    if (value > 0) return <Pill label={`${value}% MoM`} leftIcon={ArrowUpRight} size="sm" color="green" />;
    if (value < 0) return <Pill label={`${value}% MoM`} leftIcon={ArrowRight} size="sm" color="red" />;
    return <Pill label={`${value}% MoM`} size="sm" color="gray" />;
}
