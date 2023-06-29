import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';

type Props = {
    stats: Array<React.ReactNode>;
};

const StatsContainer = styled.div`
    margin-top: 8px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 5px;
`;

const StatDivider = styled.div`
    padding-left: 5px;
    margin-right: 5px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 21px;
`;

export const StatsSummary = ({ stats }: Props) => {
    return (
        <>
            {!!stats.length && (
                <StatsContainer>
                    {stats.map((statView, index) => (
                        <>
                            {statView}
                            {index < stats.length - 1 && <StatDivider />}
                        </>
                    ))}
                </StatsContainer>
            )}
        </>
    );
};
