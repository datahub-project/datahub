import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { TickLabel } from '@components/components/CalendarChart/private/components/TickLabel';
import { DAYS_IN_WEEK } from '@components/components/CalendarChart/private/constants';
import { useCalendarState } from '@components/components/CalendarChart/private/context';
import { AxisLeftWeekdaysProps } from '@components/components/CalendarChart/types';

export function AxisLeftWeekdays<ValueType>({ labelProps, showLeftAxisLine }: AxisLeftWeekdaysProps) {
    const { t } = useTranslation('alchemy');
    const themeConfig = useTheme();

    const weekdays = [
        t('calendarChart.weekday.mon'),
        t('calendarChart.weekday.tue'),
        t('calendarChart.weekday.wed'),
        t('calendarChart.weekday.thu'),
        t('calendarChart.weekday.fri'),
        t('calendarChart.weekday.sat'),
        t('calendarChart.weekday.sun'),
    ];
    const { margin, squareSize, squareGap } = useCalendarState<ValueType>();

    const yLineOffset = 5;
    const xLineOffset = 4;

    const lineHeight = squareSize + squareGap;
    const lineOffset = Math.floor(squareSize / 2) + squareGap;

    const x = margin.left - xLineOffset;
    const y = lineHeight * DAYS_IN_WEEK + margin.top + yLineOffset;

    const renderTickLabel = (number: number, text: string) => {
        const labelXOffset = 12;
        const xLabel = margin.left - labelXOffset;
        const yLabel = lineHeight * number + lineOffset + margin.top;

        return <TickLabel key={`axis-left-${number}`} text={text} x={xLabel} y={yLabel} {...(labelProps ?? {})} />;
    };

    return (
        <>
            {weekdays.map((weekday, index) => renderTickLabel(index, weekday))}
            {showLeftAxisLine && <line x1={x} x2={x} y1={0} y2={y} stroke={themeConfig.colors.border} width={1} />}
        </>
    );
}
