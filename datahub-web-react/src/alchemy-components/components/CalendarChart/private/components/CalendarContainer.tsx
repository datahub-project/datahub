import React, { useMemo } from 'react';

import { CalendarInnerWrapper, CalendarWrapper } from '@components/components/CalendarChart/components';
import { DAYS_IN_WEEK } from '@components/components/CalendarChart/private/constants';
import { useCalendarState } from '@components/components/CalendarChart/private/context';
import { CalendarContainerProps } from '@components/components/CalendarChart/types';

export function CalendarContainer<ValueType>({ children }: CalendarContainerProps) {
    const { squareSize, squareGap, margin, parentHeight, countOfWeeks, countOfMonths } = useCalendarState<ValueType>();

    const svgHeight = useMemo(
        () => Math.min((squareSize + squareGap) * DAYS_IN_WEEK + margin.top + margin.bottom, parentHeight),
        [squareSize, squareGap, margin.top, margin.bottom, parentHeight],
    );

    // Calendar can be wider then the parent container because of minimal size of square and gap
    // In this case the horizontal sroll should be shown
    const svgWidth = useMemo(
        () => (squareSize + squareGap) * countOfWeeks + squareGap * countOfMonths + margin.left + margin.right,
        [squareSize, squareGap, countOfWeeks, countOfMonths, margin.left, margin.right],
    );

    return (
        <CalendarWrapper>
            <CalendarInnerWrapper $width={`${svgWidth}px`}>
                <svg width={svgWidth} height={svgHeight}>
                    {children}
                </svg>
            </CalendarInnerWrapper>
        </CalendarWrapper>
    );
}
