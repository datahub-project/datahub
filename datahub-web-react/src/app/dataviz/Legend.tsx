import React from 'react';

import { LegendOrdinal, LegendItem, LegendLabel } from '@visx/legend';

interface Props {
    scale: any;
}

export const Legend = ({ scale }: Props) => {
    return (
        <LegendOrdinal scale={scale}>
            {(labels) => (
                <div style={{ display: 'flex', flexDirection: 'row' }}>
                    {labels.map((label) => (
                        <LegendItem
                            key={`legend-quantile-${label.text}-${label.value}-${Math.random() * 2024}`}
                            margin="0 20px 0 0"
                        >
                            <svg width={8} height={8}>
                                <rect fill={label.value} width={8} height={8} />
                            </svg>
                            <LegendLabel align="left" margin="0 0 0 5px">
                                {label.text}
                            </LegendLabel>
                        </LegendItem>
                    ))}
                </div>
            )}
        </LegendOrdinal>
    );
};
