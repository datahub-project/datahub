import { Bar } from '@visx/shape';
import styled from 'styled-components';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
`;

export const StyledBar = styled(Bar)`
    cursor: pointer;

    :hover {
        filter: drop-shadow(0px 0px 4px rgba(0 0 0 / 0.25)) brightness(90%);
    }
`;
