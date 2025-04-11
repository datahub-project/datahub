import React from 'react';
import { Text } from '@visx/text';
import { Popover } from '../../Popover';
import { TruncatableTickProps } from '../types';

export default function TruncatableTick({ formattedValue, limit, ...textProps }: TruncatableTickProps) {
    if (formattedValue === undefined) return null;
    // FYI: formatted value has type `string | undefined` but when zero is ignored, formattedValue will have type Number
    if (typeof formattedValue !== 'string') return null;

    const truncatedValue = formattedValue.slice(0, limit);
    const isValueTruncated = formattedValue.length !== truncatedValue.length;
    const finalValue = truncatedValue + (isValueTruncated ? '…' : '');

    return (
        <Popover content={isValueTruncated ? formattedValue : undefined}>
            <Text {...textProps} pointerEvents="all">
                {finalValue}
            </Text>
        </Popover>
    );
}
