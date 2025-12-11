/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
