import { Margin } from '@visx/xychart';
import React, { createContext, PropsWithChildren, useContext, useMemo } from 'react';
import { DayData, MonthData } from '../types';
import { DAYS_IN_WEEK, MIN_GAP_SIZE, MIN_SQUARE_SIZE } from './constants';

export type CalendarContextState<ValueType = any> = {
    data: MonthData<ValueType>[];
    parentWidth: number;
    parentHeight: number;
    squareSize: number;
    squareGap: number;
    margin: Margin;
    countOfWeeks: number;
    countOfMonths: number;
    showPopover?: boolean;
    popoverRenderer?: (day: DayData<ValueType>) => React.ReactNode;
    colorAccessor: (value?: ValueType) => string;
    selectedDay?: string | null;
    onDayClick?: (day: DayData<ValueType>) => void;
};

export const CalendarContext = createContext<CalendarContextState | null>(null);

export type CalendarProviderProps<ValueType> = {
    data: MonthData<ValueType>[];
    width: number;
    height: number;
    margin?: Margin;
    showPopover?: boolean;
    popoverRenderer?: (day: DayData<ValueType>) => React.ReactNode;
    colorAccessor: (value?: ValueType) => string;
    selectedDay?: string | null;
    onDayClick?: (day: DayData<ValueType>) => void;
};

export function CalendarProvider<ValueType>({
    children,
    data,
    width,
    height,
    margin,
    showPopover,
    popoverRenderer,
    colorAccessor,
    selectedDay,
    onDayClick,
}: PropsWithChildren<CalendarProviderProps<ValueType>>) {
    const calendarMargin = useMemo(
        () => ({
            // additional top margin
            top: (margin?.top ?? 0) + 10,
            right: (margin?.right ?? 0) + 0,
            // additional space for bottom axis
            bottom: (margin?.bottom ?? 0) + 40,
            // additional space for left axis
            left: (margin?.left ?? 0) + 50,
        }),
        [margin],
    );

    const calendarWidth = useMemo(() => width - calendarMargin.right - calendarMargin.left, [width, calendarMargin]);
    const calendarHeight = useMemo(() => height - calendarMargin.top - calendarMargin.bottom, [height, calendarMargin]);

    const countOfWeeks = useMemo(() => data.reduce((acc, value) => acc + value.weeks.length, 0), [data]);
    const countOfMonths = useMemo(() => data.length, [data.length]);

    const squareGap = MIN_GAP_SIZE;

    const squareSize = useMemo(() => {
        // Compute size of square depending of width
        const maxSizeByWidth = Math.floor(
            (calendarWidth - squareGap * Math.floor(countOfWeeks) - squareGap * Math.floor(countOfMonths)) /
                Math.floor(countOfWeeks),
        );

        // Compute size of square depending of height
        const maxSizeByHeight = Math.floor((calendarHeight - squareGap * DAYS_IN_WEEK) / DAYS_IN_WEEK);

        return Math.max(Math.min(maxSizeByWidth, maxSizeByHeight), MIN_SQUARE_SIZE);
    }, [squareGap, countOfWeeks, countOfMonths, calendarHeight, calendarWidth]);

    return (
        <CalendarContext.Provider
            value={{
                data,
                parentWidth: width,
                parentHeight: height,
                squareSize,
                squareGap,
                countOfWeeks,
                countOfMonths,
                colorAccessor,
                showPopover,
                popoverRenderer,
                margin: calendarMargin,
                selectedDay,
                onDayClick,
            }}
        >
            {children}
        </CalendarContext.Provider>
    );
}

export function useCalendarState<ValueType>() {
    const context = useContext<CalendarContextState<ValueType> | null>(CalendarContext);
    if (context === null) throw new Error(`${useCalendarState.name} must be used under a ${CalendarProvider.name}`);
    return context;
}
