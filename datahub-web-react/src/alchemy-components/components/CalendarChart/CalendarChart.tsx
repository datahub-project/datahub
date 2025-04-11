import { colors } from '@src/alchemy-components/theme';
import { ParentSize } from '@visx/responsive';
import React, { useMemo } from 'react';
import { ChartWrapper } from './components';
import { AxisBottomMonths } from './private/components/AxisBottomMonths';
import { AxisLeftWeekdays } from './private/components/AxisLeftWeekdays';
import { Calendar } from './private/components/Calendar';
import { CalendarProvider } from './private/context';
import { CalendarChartProps } from './types';
import { prepareCalendarData } from './utils';
import { CalendarContainer } from './private/components/CalendarContainer';

const commonLabelProps = {
    fill: colors.gray[1700],
    fontFamily: 'Mulish',
    fontSize: 10,
};

export const calendarChartDefault: Omit<CalendarChartProps<any>, 'colorAccessor' | 'startDate' | 'endDate'> = {
    data: [],
    leftAxisLabelProps: {
        ...commonLabelProps,
        textAnchor: 'end',
    },
    showLeftAxisLine: false,
    bottomAxisLabelProps: {
        ...commonLabelProps,
        textAnchor: 'start',
    },
    maxHeight: 350,
    showPopover: true,
};

export function CalendarChart<ValueType = any>({
    data = calendarChartDefault.data,
    startDate,
    endDate,
    colorAccessor,
    showPopover = calendarChartDefault.showPopover,
    popoverRenderer,
    leftAxisLabelProps = calendarChartDefault.leftAxisLabelProps,
    showLeftAxisLine = calendarChartDefault.showLeftAxisLine,
    bottomAxisLabelProps = calendarChartDefault.bottomAxisLabelProps,
    margin,
    maxHeight = calendarChartDefault.maxHeight,
    selectedDay,
    onDayClick,
}: CalendarChartProps<ValueType>) {
    const preparedData = useMemo(
        () => prepareCalendarData<ValueType>(data, startDate, endDate),
        [data, startDate, endDate],
    );

    return (
        <ChartWrapper>
            <ParentSize>
                {({ width, height }) => {
                    return (
                        <CalendarProvider<ValueType>
                            data={preparedData}
                            width={width}
                            height={maxHeight ?? height}
                            margin={margin}
                            colorAccessor={colorAccessor}
                            showPopover={showPopover}
                            popoverRenderer={popoverRenderer}
                            selectedDay={selectedDay}
                            onDayClick={onDayClick}
                        >
                            <CalendarContainer>
                                <AxisLeftWeekdays<ValueType>
                                    labelProps={leftAxisLabelProps}
                                    showLeftAxisLine={showLeftAxisLine}
                                />
                                <AxisBottomMonths<ValueType> labelProps={bottomAxisLabelProps} />

                                <Calendar<ValueType> data={preparedData} />
                            </CalendarContainer>
                        </CalendarProvider>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
}
