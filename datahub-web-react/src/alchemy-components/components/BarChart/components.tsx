import { colors } from '@src/alchemy-components/theme';
import { BarSeries } from '@visx/xychart';
import styled from 'styled-components';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
`;

export const StyledBarSeries = styled(BarSeries)<{
    $hasSelectedItem?: boolean;
    $color?: string;
    $selectedColor?: string;
}>`
    & {
        cursor: pointer;

        fill: ${(props) => (props.$hasSelectedItem ? props.$selectedColor : props.$color) || colors.violet[500]};
        ${(props) => props.$hasSelectedItem && 'opacity: 0.3;'}

        :hover {
            fill: ${(props) => props.$selectedColor || colors.violet[500]};
            filter: drop-shadow(0px -2px 5px rgba(33, 23, 95, 0.3));
            opacity: 1;
        }

        :focus {
            fill: ${(props) => props.$selectedColor || colors.violet[500]};
            outline: none;
            opacity: 1;
        }
    }
`;
