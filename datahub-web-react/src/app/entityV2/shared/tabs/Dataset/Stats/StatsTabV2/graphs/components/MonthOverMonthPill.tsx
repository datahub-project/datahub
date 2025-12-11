/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Pill } from '@components';
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

    if (value > 0) return <Pill label={`${value}% MoM`} leftIcon="TrendingUp" size="sm" color="green" />;
    if (value < 0) return <Pill label={`${value}% MoM`} leftIcon="TrendingDown" size="sm" color="red" />;
    return <Pill label={`${value}% MoM`} size="sm" color="gray" />;
}
