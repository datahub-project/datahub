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
