import { useMemo } from 'react';
import { DatePickerVariant } from '../constants';
import { VariantProps } from '../types';
import { CommonVariantProps, DateSwitcherVariantProps } from '../variants';

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
