import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';

type Props = {
    stats: Array<React.ReactNode>;
    shouldWrap?: boolean;
};

const StatsContainer = styled.div<{ shouldWrap?: boolean }>`
    margin-top: 8px;
    display: flex;
    align-items: center;
    ${(props) => props.shouldWrap && `flex-wrap: wrap;`}
`;

const StatDivider = styled.div`
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 21px;
`;

export const StatsSummary = ({ stats, shouldWrap }: Props) => {
    return (
        <>
            {stats && stats.length > 0 && (
                <StatsContainer shouldWrap={shouldWrap}>
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
