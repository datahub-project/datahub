import React, { useCallback } from 'react';
import { useTheme } from 'styled-components';

import { GLYPH_DROP_SHADOW_FILTER } from '@components/components/LineChart/constants';
import { Popover } from '@components/components/Popover';
import { Text } from '@components/components/Text';
import {
    WHISKER_METRIC_ATTRIBUTE_NAMES,
    WHISKER_METRIC_NAMES as WHISKER_METRIC_LABELS,
    getDefaultColorScheme,
} from '@components/components/WhiskerChart/constants';
import { TooltipRendererProps } from '@components/components/WhiskerChart/types';

const RADIUS = 6;

export default function GlyphWithLineAndPopover({ x, y, minY, maxY, datum }: TooltipRendererProps) {
    const theme = useTheme();
    const renderPopoverContent = useCallback(() => {
        if (!datum) return null;

        const label = WHISKER_METRIC_LABELS[datum.type];
        const value = datum[WHISKER_METRIC_ATTRIBUTE_NAMES[datum.type]];

        return (
            <>
                <Text type="span">{label}:&nbsp;</Text>
                <Text weight="semiBold" type="span">
                    {value}
                </Text>
            </>
        );
    }, [datum]);

    if (y === undefined || x === undefined) return null;

    const color = datum?.colorShemeSettings?.alternative ?? getDefaultColorScheme(theme).alternative;

    return (
        <svg>
            <line
                y1={minY}
                y2={y - RADIUS}
                x1={x}
                x2={x}
                strokeWidth={3}
                stroke={color}
                style={{ filter: GLYPH_DROP_SHADOW_FILTER, pointerEvents: 'none' }}
            />
            <line
                y1={y + RADIUS}
                y2={maxY}
                x1={x}
                x2={x}
                strokeWidth={3}
                stroke={color}
                style={{ filter: GLYPH_DROP_SHADOW_FILTER, pointerEvents: 'none' }}
            />
            <Popover content={() => renderPopoverContent()} placement="topLeft" align={{ offset: [15, 15] }} open>
                <circle
                    cx={x}
                    cy={y}
                    r={RADIUS}
                    fill="transparent"
                    strokeWidth={2}
                    stroke={color}
                    style={{ filter: GLYPH_DROP_SHADOW_FILTER, pointerEvents: 'none' }}
                />
            </Popover>
        </svg>
    );
}
