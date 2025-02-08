import { BarSeries } from '@visx/xychart';
import styled from 'styled-components';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
`;

export const StyledBarSeries = styled(BarSeries)<{
    $hasSelectedItem?: boolean;
    $isEmpty?: boolean;
}>`
    & {
        cursor: pointer;

        ${(props) => props.$isEmpty && 'pointer-events: none;'}

        ${(props) => props.$hasSelectedItem && 'opacity: 0.3;'}

        :hover {
            filter: drop-shadow(0px -2px 5px rgba(33, 23, 95, 0.3));
            opacity: 1;
        }

        :focus {
            outline: none;
            opacity: 1;
        }
    }
`;
