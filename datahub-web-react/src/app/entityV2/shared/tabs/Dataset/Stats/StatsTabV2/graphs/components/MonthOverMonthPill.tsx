import { Pill } from '@components';
import { TrendDown } from '@phosphor-icons/react/dist/csr/TrendDown';
import { TrendUp } from '@phosphor-icons/react/dist/csr/TrendUp';
import React from 'react';
import { useTranslation } from 'react-i18next';

type PillMoMProps = {
    value?: number | null;
};

// FYI: The month over month functionality is temporary disabled
// see `../utils->addMonthOverMonthValue`
const IS_MOM_PILL_DISABLED = true;

export default function MonthOverMonthPill({ value }: PillMoMProps) {
    const { t } = useTranslation('entity.profile.stats');
    if (IS_MOM_PILL_DISABLED) return null;

    if (value === undefined || value === null) return null;

    if (value > 0)
        return <Pill label={t('monthOverMonthPill.label', { value })} leftIcon={TrendUp} size="sm" color="green" />;
    if (value < 0)
        return <Pill label={t('monthOverMonthPill.label', { value })} leftIcon={TrendDown} size="sm" color="red" />;
    return <Pill label={t('monthOverMonthPill.label', { value })} size="sm" color="gray" />;
}
