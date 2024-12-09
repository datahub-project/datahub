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
    bottomAxisLabelProps: {
        ...commonLabelProps,
        textAnchor: 'middle',
    },
};

export function CalendarChart<ValueType = any>({
    data = calendarChartDefault.data,
    startDate,
    endDate,
    colorAccessor,
    popoverRenderer,
    leftAxisLabelProps = calendarChartDefault.leftAxisLabelProps,
    bottomAxisLabelProps = calendarChartDefault.bottomAxisLabelProps,
    margin,
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
                        <svg width={width} height={height}>
                            <CalendarProvider<ValueType>
                                data={preparedData}
                                width={width}
                                height={height}
                                margin={margin}
                                colorAccessor={colorAccessor}
                                popoverRenderer={popoverRenderer}
                            >
                                <AxisLeftWeekdays<ValueType> labelProps={leftAxisLabelProps} />
                                <AxisBottomMonths<ValueType> labelProps={bottomAxisLabelProps} />

                                <Calendar<ValueType> data={preparedData} />
                            </CalendarProvider>
                        </svg>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
}
