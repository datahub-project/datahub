/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Text } from '@visx/text';
import React from 'react';

import { TruncatableTickProps } from '@components/components/BarChart/types';
import { Popover } from '@components/components/Popover';

export default function TruncatableTick({ formattedValue, limit, ...textProps }: TruncatableTickProps) {
    if (formattedValue === undefined) return null;
    const formattedStringValue = formattedValue.toString();
    const truncatedValue = formattedStringValue.slice(0, limit);
    const isValueTruncated = formattedStringValue.length !== truncatedValue.length;
    const finalValue = truncatedValue + (isValueTruncated ? '…' : '');

    return (
        <Popover content={isValueTruncated ? formattedStringValue : undefined}>
            <Text {...textProps} pointerEvents="all">
                {finalValue}
            </Text>
        </Popover>
    );
}
