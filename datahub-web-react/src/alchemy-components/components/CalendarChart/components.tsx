import { Bar } from '@visx/shape';
import styled from 'styled-components';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
`;

export const CalendarWrapper = styled.div`
    width: 100%;
    height: 100%;
    display: flex;
    justify-content: left;
    overflow-x: auto;
`;

export const CalendarInnerWrapper = styled.div<{ $width: string }>`
    width: ${(props) => props.$width};
`;

export const StyledBar = styled(Bar)<{ $addTransparency?: boolean }>`
    cursor: pointer;

    ${(props) => props.$addTransparency && 'filter: opacity(0.3);'}

    :hover {
        filter: drop-shadow(0px 0px 4px rgba(0 0 0 / 0.25)) brightness(90%);
    }
`;
