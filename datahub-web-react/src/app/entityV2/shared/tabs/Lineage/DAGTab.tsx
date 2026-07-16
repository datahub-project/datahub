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
