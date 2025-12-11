/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

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
