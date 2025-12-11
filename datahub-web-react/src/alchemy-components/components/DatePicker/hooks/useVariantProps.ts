/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
