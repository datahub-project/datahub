/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { VariantProps } from '@components/components/DatePicker/types';
import { CommonVariantProps } from '@components/components/DatePicker/variants/common/props';
import { DateSwitcherInput } from '@components/components/DatePicker/variants/dateSwitcher/components';

export const DateSwitcherVariantProps: VariantProps = {
    ...CommonVariantProps,
    inputRender: (props) => <DateSwitcherInput {...props} />,
};
