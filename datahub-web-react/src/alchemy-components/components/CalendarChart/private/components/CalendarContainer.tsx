import React, { useMemo } from 'react';
import { useCalendarState } from '../context';
import { DAYS_IN_WEEK } from '../constants';
import { CalendarContainerProps } from '../../types';
import { CalendarInnerWrapper, CalendarWrapper } from '../../components';

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
