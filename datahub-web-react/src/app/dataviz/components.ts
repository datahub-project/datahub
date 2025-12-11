/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;

    .horizontalBarChartTick {
        foreignObject {
            text-align: right;
        }
    }

    .horizontalBarChartInlineLabel {
        fill: #fff;
        font-weight: 600;
        font-family: 'Manrope', sans-serif;
    }

    .visx-axis-label {
        font-weight: 600 !important;
        font-family: 'Manrope', sans-serif !important;
    }
`;
