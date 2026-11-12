import { LegendItem, LegendLabel, LegendOrdinal } from '@visx/legend';
import { ScaleOrdinal } from 'd3-scale/src/ordinal';
import React from 'react';
import styled from 'styled-components';

const legendGlyphSize = 10;

type Props = {
    ordinalScale: ScaleOrdinal<string, string>;
};

const LegendRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    width: 100%;
`;

const LegendCol = styled.div`
    flex: 0 0 33.33333333%;
    max-width: 33.33333333%;
`;

export default function Legend({ ordinalScale }: Props) {
    return (
        <LegendRow>
            <LegendOrdinal scale={ordinalScale} labelFormat={(d: any) => d}>
                {(labels) => {
                    return labels.map((label) => (
                        <LegendCol>
                            <LegendItem key={`legend-quantile-${label}`}>
                                <svg width={legendGlyphSize} height={legendGlyphSize} style={{ margin: '2px 0' }}>
                                    <circle
                                        fill={label.value}
                                        r={legendGlyphSize / 2}
                                        cx={legendGlyphSize / 2}
                                        cy={legendGlyphSize / 2}
                                    />
                                </svg>
                                <LegendLabel align="left" margin="0 4px">
                                    {label.text}
                                </LegendLabel>
                            </LegendItem>
                        </LegendCol>
                    ));
                }}
            </LegendOrdinal>
        </LegendRow>
    );
}
