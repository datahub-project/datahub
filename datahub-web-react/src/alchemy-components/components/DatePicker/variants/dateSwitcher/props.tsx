import React from 'react';

import { VariantProps } from '@components/components/DatePicker/types';
import { CommonVariantProps } from '@components/components/DatePicker/variants/common/props';
import { DateSwitcherInput } from '@components/components/DatePicker/variants/dateSwitcher/components';

export const DateSwitcherVariantProps: VariantProps = {
    ...CommonVariantProps,
    bordered: false,
    allowClear: false,
    format: 'll',
    suffixIcon: null,
    inputRender: (props) => <DateSwitcherInput {...props} />,
    $noDefaultPaddings: true,
};
