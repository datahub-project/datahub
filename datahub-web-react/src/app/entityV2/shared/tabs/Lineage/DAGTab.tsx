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

import LineageGraph from '@app/lineageV2/LineageGraph';
import LineageGraphContext from '@app/lineageV3/LineageGraphContext';

const VisualizationWrapper = styled.div`
    display: flex;
    height: 100%;
`;

export function DAGTab() {
    return (
        <LineageGraphContext.Provider value={{ isDAGView: true }}>
            <VisualizationWrapper>
                <LineageGraph />
            </VisualizationWrapper>
        </LineageGraphContext.Provider>
    );
}
