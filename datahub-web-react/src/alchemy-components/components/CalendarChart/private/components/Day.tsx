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
            <Popover placement="topLeft" content={popoverRenderer?.(day)}>
                {renderBar()}
            </Popover>
        );
    }
    return renderBar();
}
