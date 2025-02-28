import React, { useCallback } from 'react';
import { Popover } from '../../Popover';
import { Text } from '../../Text';
import {
    DEFAULT_COLOR_SHEME,
    WHISKER_METRIC_ATTRIBUTE_NAMES,
    WHISKER_METRIC_NAMES as WHISKER_METRIC_LABELS,
} from '../constants';
import { TooltipRendererProps } from '../types';

const RADIUS = 6;

const SHADOW = `
    drop-shadow(0px 1px 3px rgba(33, 23, 95, 0.30))
    drop-shadow(0px 2px 5px rgba(33, 23, 95, 0.25))
    drop-shadow(0px -2px 5px rgba(33, 23, 95, 0.25))
`;

export default function GlyphWithLineAndPopover({ x, y, minY, maxY, datum }: TooltipRendererProps) {
    const renderPopoverContent = useCallback(() => {
        if (!datum) return null;

        const label = WHISKER_METRIC_LABELS[datum.type];
        const value = datum[WHISKER_METRIC_ATTRIBUTE_NAMES[datum.type]];

        return (
            <>
                <Text type="span" color="gray">
                    {label}:&nbsp;
                </Text>
                <Text weight="semiBold" color="gray" type="span">
                    {value}
                </Text>
            </>
        );
    }, [datum]);

    if (y === undefined || x === undefined) return null;

    const color = datum?.colorShemeSettings?.alternative ?? DEFAULT_COLOR_SHEME.alternative;

    return (
        <svg>
            <line
                y1={minY}
                y2={y - RADIUS}
                x1={x}
                x2={x}
                strokeWidth={3}
                stroke={color}
                style={{ filter: SHADOW, pointerEvents: 'none' }}
            />
            <line
                y1={y + RADIUS}
                y2={maxY}
                x1={x}
                x2={x}
                strokeWidth={3}
                stroke={color}
                style={{ filter: SHADOW, pointerEvents: 'none' }}
            />
            <Popover content={() => renderPopoverContent()} placement="topLeft" align={{ offset: [15, 15] }} open>
                <circle
                    cx={x}
                    cy={y}
                    r={RADIUS}
                    fill="transparent"
                    strokeWidth={2}
                    stroke={color}
                    style={{ filter: SHADOW, pointerEvents: 'none' }}
                />
            </Popover>
        </svg>
    );
}
