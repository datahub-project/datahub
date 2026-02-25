import { Popover } from '@components';
import { AxisBottom } from '@visx/axis';
import { Group } from '@visx/group';
import { scaleUtc } from '@visx/scale';
import { Bar } from '@visx/shape';
import { Maybe } from 'graphql/jsutils/Maybe';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

import { LinkWrapper } from '@app/shared/LinkWrapper';
import ColorTheme from '@src/conf/theme/colorThemes/types';

import { AssertionResultType } from '@types';

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

const getFillColor = (type: AssertionResultType, colors: ColorTheme) => {
    switch (type) {
        case AssertionResultType.Success:
            return colors.iconSuccess;
        case AssertionResultType.Failure:
            return colors.iconError;
        case AssertionResultType.Error:
            return colors.iconWarning;
        case AssertionResultType.Init:
            return colors.iconDisabled;
        default:
            throw new Error(`Unsupported Assertion Result Type ${type} provided.`);
    }
};

/**
 * Assertion run results displayed on a horizontal timeline.
 */
export const AssertionResultTimeline = ({ data, timeRange, width }: Props) => {
    const theme = useTheme();
    const yMax = 60;
    const left = 0;

    const xScale = useMemo(
        () =>
            scaleUtc({
                domain: [new Date(timeRange.startMs), new Date(timeRange.endMs + 600000)],
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
                        const barHeight = 18;
                        const barX = xScale(new Date(d.time));
                        const barY = yMax - barHeight;
                        const fillColor = getFillColor(d.type, theme.colors);
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
                                >
                                    <Bar
                                        key={`bar-${d.time}`}
                                        x={barX}
                                        y={barY}
                                        stroke={theme.colors.bg}
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
                    numTicks={7}
                    stroke={theme.colors.border}
                    tickFormat={(v: any) => v.toLocaleDateString('en-us', { month: 'short', day: 'numeric' })}
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
