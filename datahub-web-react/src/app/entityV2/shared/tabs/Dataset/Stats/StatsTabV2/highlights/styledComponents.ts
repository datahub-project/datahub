import { Divider } from 'antd';
import styled, { css } from 'styled-components';

export const CARD_WIDTH = '225px';
export const CARD_HEIGHT = '90px';

const NUM_CARDS_LATEST_STATS = 2;
const NUM_CARDS_LAST_MONTH_STATS = 3;
const LATEST_STATS_MAX_WIDTH = 470;

export const highlightsSectionStyles = css`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export const LatestStatsContainer = styled.div`
    ${highlightsSectionStyles};
    max-width: ${LATEST_STATS_MAX_WIDTH}px;
    flex: ${NUM_CARDS_LATEST_STATS};
`;

export const LastMonthStatsContainer = styled.div`
    ${highlightsSectionStyles}
    flex: ${NUM_CARDS_LAST_MONTH_STATS};
`;

export const Header = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const StatsContainer = styled.div`
    display: flex;
    padding: 12px 0;
    width: 100%;
    box-sizing: border-box;

    overflow-x: auto;
    &::-webkit-scrollbar {
        display: none;
    }
`;

export const StatCards = styled.div`
    display: flex;
    gap: 20px;
`;

export const VerticalDivider = styled(Divider)`
    height: auto;
    margin: 0 20px;
    align-self: stretch;
`;
