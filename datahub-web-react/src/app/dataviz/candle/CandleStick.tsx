import React from 'react';

import { Group } from '@visx/group';
import { Bar } from '@visx/shape';
import { BarProps } from '@visx/shape/lib/shapes/Bar';
import { GlyphCircle, GlyphDiamond } from '@visx/glyph';
import { GlyphDiamondProps } from '@visx/glyph/lib/glyphs/GlyphDiamond';
import { GlyphCircleProps } from '@visx/glyph/lib/glyphs/GlyphCircle';
import { AddSVGProps } from '@visx/shape/lib/types';

type DiamondProps = GlyphDiamondProps<any> & Omit<React.SVGProps<SVGPathElement>, keyof GlyphDiamondProps<any>>;
type CircleProps = GlyphCircleProps<any> & Omit<React.SVGProps<SVGPathElement>, keyof GlyphCircleProps<any>>;
type CandleBarProps = AddSVGProps<BarProps, SVGRectElement>;

type Props = {
    parentChartHeight: number;
    candleHeight: number;
    barWidth: number;
    shapeSize: number;
    leftOffset: number;
    color: string;
    shape:
        | {
              type: 'diamond';
              extraProps?: DiamondProps;
          }
        | {
              type: 'circle';
              extraProps?: CircleProps;
          };
    markerOverlapPx?: number;
    extraBarProps?: CandleBarProps;
    opacity?: number;
    wrapper?: (children: JSX.Element) => JSX.Element;
};
export const CandleStick = ({
    parentChartHeight,
    candleHeight,
    barWidth,
    shapeSize,
    leftOffset,
    color,
    shape,
    wrapper,
    opacity,
    markerOverlapPx,
    extraBarProps,
}: Props) => {
    const yOffset = parentChartHeight - candleHeight;

    const shapeProps: DiamondProps | CircleProps = {
        top: yOffset,
        left: leftOffset,
        fill: color,
        stroke: 'white',
        strokeWidth: (markerOverlapPx ?? 1) > 1 ? 1 / (markerOverlapPx ?? 1) : 1,
        filter: markerOverlapPx ? undefined : 'drop-shadow(0px 1px 2.5px rgb(0 0 0 / 0.1))',
        size: shapeSize,
        ...shape.extraProps,
    };
    const barProps: CandleBarProps = {
        height: candleHeight,
        width: barWidth,
        x: leftOffset - barWidth / 2,
        y: yOffset,
        fill: color,
        stroke: 'white',
        strokeWidth: (markerOverlapPx ?? 1) > 1 ? 1 / (markerOverlapPx ?? 1) : 1,
        ...extraBarProps,
    };

    const candleGroup = (
        <Group opacity={opacity}>
            <Bar {...barProps} />
            {shape.type === 'diamond' ? <GlyphDiamond {...shapeProps} /> : <GlyphCircle {...shapeProps} />}
        </Group>
    );
    return wrapper ? wrapper(candleGroup) : candleGroup;
};
