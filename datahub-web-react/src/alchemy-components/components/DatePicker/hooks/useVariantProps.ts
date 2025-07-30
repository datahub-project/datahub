import { useMemo } from 'react';

import { DatePickerVariant } from '@components/components/DatePicker/constants';
import { VariantProps } from '@components/components/DatePicker/types';
import { CommonVariantProps, DateSwitcherVariantProps } from '@components/components/DatePicker/variants';

export default function useVariantProps(variant: DatePickerVariant | undefined): VariantProps {
    return useMemo(() => {
        switch (variant) {
            case DatePickerVariant.DateSwitcher:
                return DateSwitcherVariantProps;
            default:
                return CommonVariantProps;
        }
    }, [variant]);
}
