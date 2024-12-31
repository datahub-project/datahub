import React from 'react';
import { Col, Row } from 'antd';
import { LegendOrdinal, LegendItem, LegendLabel } from '@visx/legend';
import { ScaleOrdinal } from 'd3-scale/src/ordinal';
import styled from 'styled-components';

const legendGlyphSize = 10;

type Props = {
    ordinalScale: ScaleOrdinal<string, string>;
};

const LegendRow = styled(Row)`
    width: 100%;
`;

export default function Legend({ ordinalScale }: Props) {
    return (
        <LegendRow>
            <LegendOrdinal scale={ordinalScale} labelFormat={(d: any) => d}>
                {(labels) => {
                    return labels.map((label) => (
                        <Col span={8}>
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
                        </Col>
                    ));
                }}
            </LegendOrdinal>
        </LegendRow>
    );
}
