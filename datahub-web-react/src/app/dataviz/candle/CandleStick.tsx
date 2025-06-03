import { GlyphCircle, GlyphDiamond } from '@visx/glyph';
import { GlyphCircleProps } from '@visx/glyph/lib/glyphs/GlyphCircle';
import { GlyphDiamondProps } from '@visx/glyph/lib/glyphs/GlyphDiamond';
import { Group } from '@visx/group';
import { Bar } from '@visx/shape';
import { BarProps } from '@visx/shape/lib/shapes/Bar';
import { AddSVGProps } from '@visx/shape/lib/types';
import React from 'react';

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
    showWarningOverlay?: boolean;
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
    showWarningOverlay,
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
            {/* For Smart Assertions: '!' icon overlaps if this has been marked as a false positive or false negative */}
            {showWarningOverlay ? (
                // Downloaded from phosphor icons
                <svg
                    x={leftOffset - shapeSize / 20}
                    y={yOffset - shapeSize / 20}
                    width={shapeSize / 10}
                    height={shapeSize / 10}
                    fill="white"
                    viewBox="0 0 256 256"
                >
                    <path d="M144,200a16,16,0,1,1-16-16A16,16,0,0,1,144,200Zm-16-40a8,8,0,0,0,8-8V48a8,8,0,0,0-16,0V152A8,8,0,0,0,128,160Z" />
                </svg>
            ) : null}
        </Group>
    );
    return wrapper ? wrapper(candleGroup) : candleGroup;
};
