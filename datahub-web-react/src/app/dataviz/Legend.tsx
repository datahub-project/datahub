/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LegendItem, LegendLabel, LegendOrdinal } from '@visx/legend';
import React from 'react';

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
