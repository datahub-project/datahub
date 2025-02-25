import { Margin } from '@visx/xychart';
import { SVGAttributes } from 'react';

export type DayData<ValueType> = {
    day: string;
    key: string;
    value?: ValueType;
};

export type WeekData<ValueType> = {
    days: DayData<ValueType>[];
    key: string;
};

export type MonthData<ValueType> = {
    weeks: WeekData<ValueType>[];
    key: string;
    label: string;
};

export type CalendarData<ValueType> = {
    day: string;
    value: ValueType;
};

export type LabelProps = Omit<SVGAttributes<SVGTextElement>, 'x, y'>;

export type Accessor<ValueType, ResponseType> = (value: ValueType) => ResponseType;

export type ColorAccessor<ValueType> = {
    valueAccessor: Accessor<ValueType, number>;
    colors: string[];
};

export type CalendarChartProps<ValueType> = {
    data: CalendarData<ValueType>[];
    startDate: string | Date;
    endDate: string | Date;
    colorAccessor: (value: any) => string;
    showPopover?: boolean;
    popoverRenderer?: (day: DayData<ValueType>) => React.ReactNode;
    leftAxisLabelProps?: LabelProps;
    showLeftAxisLine?: boolean;
    bottomAxisLabelProps?: LabelProps;
    margin?: Margin;
    maxHeight?: number;
    selectedDay?: string | null;
    onDayClick?: (day: DayData<ValueType>) => void;
};

export type CalendarProps<ValueType> = {
    data: MonthData<ValueType>[];
};

export type MonthProps<ValueType> = {
    month: MonthData<ValueType>;
    monthIndex: number;
};

export type WeekProps<ValueType> = {
    week: WeekData<ValueType>;
    weekNumber: number;
    monthOffset: number;
};

export type DayProps<ValueType> = {
    day: DayData<ValueType>;
    weekOffset: number;
    dayIndex: number;
};

export type AxisLeftWeekdaysProps = {
    labelProps?: LabelProps;
    showLeftAxisLine?: boolean;
};

export type AxisBottomMonthsProps = {
    labelProps?: LabelProps;
};

export type CalendarContainerProps = {
    children: React.ReactNode;
};
