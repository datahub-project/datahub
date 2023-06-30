import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';

type Props = {
    stats: Array<React.ReactNode>;
};

const StatsContainer = styled.div`
    position: relative;
`;

const StatsLeftBorderHider = styled.div`
    position: absolute;
    left: -10px;
    border: 2px solid red;
    z-index: 1;
    height: 100%;
`;

const StatsListContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 8px;
    margin-left: -10px;
`;

const StatContainer = styled.div`
    /* Flex needed so the child stats can animate */
    display: flex;
    padding-left: 10px;
    border-left: 1px solid ${ANTD_GRAY[4]};
`;

export const StatsSummary = ({ stats }: Props) => {
    return (
        <StatsContainer>
            <StatsLeftBorderHider />
            {!!stats.length && (
                <StatsListContainer>
                    {stats.map((statView) => (
                        <StatContainer>{statView}</StatContainer>
                    ))}
                </StatsListContainer>
            )}
        </StatsContainer>
    );
};
