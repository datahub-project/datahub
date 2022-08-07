import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';

type Props = {
    stats: Array<React.ReactNode>;
};

const StatsContainer = styled.div`
    margin-top: 8px;
`;

const StatDivider = styled.div`
    display: inline-block;
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 21px;
    vertical-align: text-top;
`;

export const StatsSummary = ({ stats }: Props) => {
    return (
        <>
            {stats && stats.length > 0 && (
                <StatsContainer>
                    {stats.map((statView, index) => (
                        <span>
                            {statView}
                            {index < stats.length - 1 && <StatDivider />}
                        </span>
                    ))}
                </StatsContainer>
            )}
        </>
    );
};
