import React from 'react';
import { DAYS_IN_WEEK } from '../constants';
import { AxisLeftWeekdaysProps } from '../../types';
import { useCalendarState } from '../context';
import { TickLabel } from './TickLabel';

const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thur', 'Fri', 'Sat', 'Sun'];

export function AxisLeftWeekdays<ValueType>({ labelProps, showLeftAxisLine }: AxisLeftWeekdaysProps) {
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
            {WEEKDAYS.map((weekday, index) => renderTickLabel(index, weekday))}
            {showLeftAxisLine && <line x1={x} x2={x} y1={0} y2={y} stroke="#EBECF0" width={1} />}
        </>
    );
}
