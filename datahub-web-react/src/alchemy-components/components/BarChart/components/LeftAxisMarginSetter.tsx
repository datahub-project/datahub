import { DataContext } from '@visx/xychart';
import { useCallback, useContext, useEffect } from 'react';
import { getTicks } from '@visx/scale';
import { TickFormatter } from '@visx/axis';

interface LeftAxisMarginSetterProps {
    setLeftMargin?: (margin: number) => void;
    numOfTicks?: number;
    formatter: TickFormatter<number>;
    charWidth?: number;
    maxMargin?: number;
}

const DEFAULT_CHAR_WIDTH = 7.5;
const MINIMAL_LABEL_LENGTH = 1;

/**
 * FYI: to get real ticks (for approximate calculation of maximum label width) that will be shown on the axis,
 * we should get the final scale object that available only in DataContext under XYChart
 */
export default function LeftAxisMarginSetter({
    setLeftMargin,
    numOfTicks,
    formatter,
    charWidth,
    maxMargin,
}: LeftAxisMarginSetterProps) {
    const { yScale } = useContext(DataContext);

    const computeMargin = useCallback(
        (ticks: number[]) => {
            const maxLengthOfLabel = Math.max(
                ...ticks
                    .map((tick, index) => ({ value: tick, index }))
                    .map(
                        (tick, index, items) =>
                            formatter(tick.value, index, items)?.toString()?.length ?? MINIMAL_LABEL_LENGTH,
                    ),
            );

            const margin = Math.ceil(maxLengthOfLabel * (charWidth ?? DEFAULT_CHAR_WIDTH));

            if (maxMargin) return Math.min(margin, maxMargin);

            return margin;
        },
        [formatter, charWidth, maxMargin],
    );

    useEffect(() => {
        if (yScale) {
            setLeftMargin?.(computeMargin(getTicks(yScale, numOfTicks)));
        }
    }, [yScale, setLeftMargin, numOfTicks, computeMargin]);

    return null;
}
