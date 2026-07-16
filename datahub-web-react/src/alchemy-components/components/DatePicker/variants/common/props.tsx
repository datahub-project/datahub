import React from 'react';

import { StyledCalendarWrapper } from '@components/components/DatePicker/components';
import { VariantProps } from '@components/components/DatePicker/types';
import { DefaultDatePickerInput } from '@components/components/DatePicker/variants/common/components';

export const CommonVariantProps: VariantProps = {
    panelRender: (panel) => <StyledCalendarWrapper>{panel}</StyledCalendarWrapper>,
    inputRender: (props) => <DefaultDatePickerInput {...props} />,
    bordered: false,
    allowClear: false,
    format: 'll',
    suffixIcon: null,
    $noDefaultPaddings: true,
};
