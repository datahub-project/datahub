import React, { useMemo } from 'react';

import { StyledBar } from '@components/components/CalendarChart/components';
import { useCalendarState } from '@components/components/CalendarChart/private/context';
import { DayProps } from '@components/components/CalendarChart/types';

import { Popover } from '@src/alchemy-components/components/Popover';

export function Day<ValueType>({ day, weekOffset, dayIndex }: DayProps<ValueType>) {
    const { squareSize, squareGap, margin, colorAccessor, showPopover, popoverRenderer, selectedDay, onDayClick } =
        useCalendarState<ValueType>();
    const color = useMemo(() => colorAccessor(day.value), [colorAccessor, day.value]);

    const y = useMemo(
        () => (squareGap + squareSize) * dayIndex + margin.top,
        [squareGap, squareSize, dayIndex, margin],
    );

    const renderBar = () => {
        return (
            <StyledBar
                data-testid={`day-${day.key}`}
                x={weekOffset}
                y={y}
                width={squareSize}
                height={squareSize}
                rx={4}
                fill={color}
                onPointerUp={() => onDayClick?.(day)}
                $addTransparency={!!selectedDay && selectedDay !== day.day}
            />
        );
    };

    if (showPopover) {
        return (
            <Popover
                placement="topLeft"
                content={popoverRenderer?.(day)}
                // Opens above this 16px square, which is exactly where the previous day sits.
                // The box must not catch that hover. Controls inside the content opt back in.
                overlayStyle={{ pointerEvents: 'none' }}
                overlayInnerStyle={{ pointerEvents: 'none' }}
            >
                {renderBar()}
            </Popover>
        );
    }
    return renderBar();
}
