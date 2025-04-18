import { LinearGradient } from '@visx/gradient';
import { BarRounded } from '@visx/shape';
import React, { useState } from 'react';
import { DEFAULT_COLOR_SHEME } from '../constants';
import { WhiskerMetricType, WhiskerRenderProps } from '../types';
import MetricPoint from './MetricPoint';

export default function WhiskerRenderer({
    datum,
    tooltip,
    box,
    min,
    minToFirst,
    median,
    maxToThird,
    max,
}: WhiskerRenderProps) {
    const [isMedianLineVisible, setIsMedianLineVisible] = useState<boolean>(true);

    const maxY = box.y1;
    const height = box.y2;
    const centerY = minToFirst.y1;

    // FYI: splitting the bar into two halves along the median to display different gradients on them
    const leftBarX = box.x1;
    const leftBarWidth = median.x1 - leftBarX;
    const rightBarX = median.x1;
    const rightBarWidth = maxToThird.x2 - rightBarX;

    const leftBarGradientId = `left-gradient-${datum.key}`;
    const rightBarGradientId = `right-gradient-${datum.key}`;

    const colorSheme = datum?.colorShemeSettings ?? DEFAULT_COLOR_SHEME;

    const handleMetricOver = (pointX: number, metricType: WhiskerMetricType) => {
        if (metricType === WhiskerMetricType.Median) setIsMedianLineVisible(false);

        tooltip.showTooltip({
            tooltipData: { ...datum, type: metricType },
            tooltipLeft: pointX,
            tooltipTop: centerY,
        });
    };

    const handleMetricLeave = (metricType: WhiskerMetricType) => {
        if (metricType === WhiskerMetricType.Median) setIsMedianLineVisible(true);
        tooltip.hideTooltip();
    };

    return (
        <>
            <LinearGradient
                vertical={false}
                id={leftBarGradientId}
                from={colorSheme.box}
                to={colorSheme.boxAlternative}
            />
            <LinearGradient
                vertical={false}
                id={rightBarGradientId}
                from={colorSheme.boxAlternative}
                to={colorSheme.box}
            />

            {/* Min to first quartile whisker */}
            <line {...minToFirst} strokeWidth={3} stroke={colorSheme.alternative} />
            <line {...min} strokeWidth={3} stroke={colorSheme.alternative} />
            <MetricPoint
                pointX={min.x1}
                topOfWhiskerBar={maxY}
                heightOfWhiskerBar={height}
                overHandler={() => handleMetricOver(min.x1, WhiskerMetricType.Min)}
                leaveHandler={() => handleMetricLeave(WhiskerMetricType.Min)}
            />

            {/* Left half of bar */}
            <BarRounded
                radius={4}
                left
                x={leftBarX}
                y={maxY}
                width={leftBarWidth}
                height={height}
                fill={`url(#${leftBarGradientId})`}
            />
            <MetricPoint
                pointX={leftBarX}
                topOfWhiskerBar={maxY}
                heightOfWhiskerBar={height}
                overHandler={() => handleMetricOver(leftBarX, WhiskerMetricType.FirstQuartile)}
                leaveHandler={() => handleMetricLeave(WhiskerMetricType.FirstQuartile)}
            />

            {/* Right half of bar */}
            <BarRounded
                radius={4}
                right
                x={rightBarX}
                y={maxY}
                width={rightBarWidth}
                height={height}
                fill={`url(#${rightBarGradientId})`}
            />
            <MetricPoint
                pointX={rightBarX + rightBarWidth}
                topOfWhiskerBar={maxY}
                heightOfWhiskerBar={height}
                overHandler={() => handleMetricOver(rightBarX + rightBarWidth, WhiskerMetricType.ThirdQuartile)}
                leaveHandler={() => handleMetricLeave(WhiskerMetricType.ThirdQuartile)}
            />

            {/* Median */}
            {isMedianLineVisible && (
                <line {...median} strokeWidth={3} stroke={colorSheme.medianLine} pointerEvents="visiblePainted" />
            )}
            <MetricPoint
                pointX={median.x1}
                topOfWhiskerBar={maxY}
                heightOfWhiskerBar={height}
                overHandler={() => handleMetricOver(median.x1, WhiskerMetricType.Median)}
                leaveHandler={() => handleMetricLeave(WhiskerMetricType.Median)}
            />

            {/* Third quartile to max whisker */}
            <line {...maxToThird} strokeWidth={3} stroke={colorSheme.alternative} />
            <line {...max} strokeWidth={3} stroke={colorSheme.alternative} />
            <MetricPoint
                pointX={max.x1}
                topOfWhiskerBar={maxY}
                heightOfWhiskerBar={height}
                overHandler={() => handleMetricOver(max.x1, WhiskerMetricType.Max)}
                leaveHandler={() => handleMetricLeave(WhiskerMetricType.Max)}
            />
        </>
    );
}
