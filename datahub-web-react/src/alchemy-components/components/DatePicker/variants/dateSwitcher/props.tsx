import React from 'react';
import { VariantProps } from '../../types';
import { CommonVariantProps } from '../common/props';
import { DateSwitcherInput } from './components';

export const DateSwitcherVariantProps: VariantProps = {
    ...CommonVariantProps,
    bordered: false,
    allowClear: false,
    format: 'll',
    suffixIcon: null,
    inputRender: (props) => <DateSwitcherInput {...props} />,
    $noDefaultPaddings: true,
};
