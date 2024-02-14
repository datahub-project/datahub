import React, { useMemo } from 'react';

import { Popover } from 'antd';
import { Bar } from '@visx/shape';
import { Group } from '@visx/group';
import { AxisBottom } from '@visx/axis';
import { scaleUtc } from '@visx/scale';
import { Maybe } from 'graphql/jsutils/Maybe';

import { ANTD_GRAY } from '../../../../../../../../constants';
import { LinkWrapper } from '../../../../../../../../../../shared/LinkWrapper';
import { AssertionResultType } from '../../../../../../../../../../../types.generated';
import { generateTickValues, getCustomTickValue, getFillColor } from './utils';

export type AssertionResult = {
    type: AssertionResultType;
    title: React.ReactNode;
    content: React.ReactNode;
    resultUrl?: Maybe<string>;
};

export type AssertionDataPoint = {
    time: number;
    result: AssertionResult;
};

export type TimeRange = {
    startMs: number;
    endMs: number;
};

type Props = {
    data: Array<AssertionDataPoint>;
    timeRange: TimeRange;
    width: number;
};

/**
 * Assertion run results displayed on a horizontal timeline.
 */
export const AssertionResultTimelineChart = ({ data, timeRange, width }: Props) => {
    const yMax = 60;
    const left = 0;

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs)],
                range: [0, width],
            }),
        [timeRange, width],
    );

    const transformedData = data.map((result, i) => {
        return {
            index: i,
            title: result.result.title,
            content: result.result.content,
            type: result.result.type,
            time: result.time,
            url: result.result.resultUrl,
        };
    });

    return (
        <>
            <svg width={width} height={88}>
                <Group>
                    {transformedData.map((d) => {
                        const barWidth = 8;
                        const barHeight = 28;
                        const barX = xScale(new Date(d.time));
                        const barY = yMax - barHeight;
                        const fillColor = getFillColor(d.type);
                        return (
                            <LinkWrapper to={d.url} target="_blank">
                                <Popover
                                    key={d.time}
                                    title={d.title}
                                    overlayStyle={{
                                        maxWidth: 440,
                                        wordWrap: 'break-word',
                                    }}
                                    content={d.content}
                                    showArrow={false}
                                >
                                    <Bar
                                        key={`bar-${d.time}`}
                                        x={barX}
                                        y={barY}
                                        stroke="white"
                                        width={barWidth}
                                        height={barHeight}
                                        fill={fillColor}
                                    />
                                </Popover>
                            </LinkWrapper>
                        );
                    })}
                </Group>
                <AxisBottom
                    top={yMax}
                    left={left}
                    scale={xScale}
                    stroke={ANTD_GRAY[5]}
                    tickValues={generateTickValues(timeRange.startMs, timeRange.endMs)}
                    tickFormat={(v) => getCustomTickValue(v, timeRange)}
                    tickLabelProps={(_) => ({
                        fontSize: 11,
                        angle: 0,
                        textAnchor: 'middle',
                    })}
                />
            </svg>
        </>
    );
};
