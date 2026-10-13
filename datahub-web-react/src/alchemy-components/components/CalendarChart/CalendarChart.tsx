import { ParentSize } from '@visx/responsive';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

import { ChartWrapper } from '@components/components/CalendarChart/components';
import { AxisBottomMonths } from '@components/components/CalendarChart/private/components/AxisBottomMonths';
import { AxisLeftWeekdays } from '@components/components/CalendarChart/private/components/AxisLeftWeekdays';
import { Calendar } from '@components/components/CalendarChart/private/components/Calendar';
import { CalendarContainer } from '@components/components/CalendarChart/private/components/CalendarContainer';
import { CalendarProvider } from '@components/components/CalendarChart/private/context';
import { CalendarChartProps } from '@components/components/CalendarChart/types';
import { prepareCalendarData } from '@components/components/CalendarChart/utils';

const getCommonLabelProps = (fill: string) => ({
    fill,
    fontFamily: 'Mulish',
    fontSize: 10,
});

export function CalendarChart<ValueType = any>({
    data = [],
    startDate,
    endDate,
    colorAccessor,
    showPopover = true,
    popoverRenderer,
    leftAxisLabelProps,
    showLeftAxisLine = false,
    bottomAxisLabelProps,
    margin,
    maxHeight = 350,
    selectedDay,
    onDayClick,
    dataTestId,
}: CalendarChartProps<ValueType>) {
    const theme = useTheme();
    const commonLabelProps = getCommonLabelProps(theme.colors.text);
    const preparedData = useMemo(
        () => prepareCalendarData<ValueType>(data, startDate, endDate),
        [data, startDate, endDate],
    );

    return (
        <ChartWrapper data-testid={dataTestId}>
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
                                    labelProps={leftAxisLabelProps ?? { ...commonLabelProps, textAnchor: 'end' }}
                                    showLeftAxisLine={showLeftAxisLine}
                                />
                                <AxisBottomMonths<ValueType>
                                    labelProps={bottomAxisLabelProps ?? { ...commonLabelProps, textAnchor: 'start' }}
                                />

                                <Calendar<ValueType> data={preparedData} />
                            </CalendarContainer>
                        </CalendarProvider>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
}
