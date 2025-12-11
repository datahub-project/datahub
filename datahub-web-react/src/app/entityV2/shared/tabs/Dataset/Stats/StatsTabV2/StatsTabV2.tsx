/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext, useEffect } from 'react';
import styled from 'styled-components';

import StatsSections from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSections';
import StatsHighlights from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/StatsHighlights';
import EntitySidebarContext from '@src/app/sharedV2/EntitySidebarContext';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    useEffect(() => {
        if (!isClosed) setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <>
            <TabContainer>
                <StatsHighlights />
                <StatsSections />
            </TabContainer>
        </>
    );
};

export default StatsTabV2;
