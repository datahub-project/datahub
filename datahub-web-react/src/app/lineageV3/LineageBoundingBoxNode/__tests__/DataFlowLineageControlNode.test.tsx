import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { NodeProps, ReactFlowProvider } from 'reactflow';

import DataFlowLineageControlNode, {
    DataFlowLineageControlData,
} from '@app/lineageV3/LineageBoundingBoxNode/DataFlowLineageControlNode';
import { FetchStatus, LineageEntity, LineageNodesContext, NodeContext } from '@app/lineageV3/common';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, LineageDirection } from '@types';

vi.mock('@app/lineageV3/LineageEntityNode/ContractLineageControl', () => ({
    ContractLineageControl: ({ direction }: { direction: LineageDirection }) => <div>contract {direction}</div>,
}));

vi.mock('@app/lineageV3/LineageEntityNode/ExpandLineageButton', () => ({
    ExpandLineageButton: ({ direction, count }: { direction: LineageDirection; count: number }) => (
        <div>
            expand {direction} {count}
        </div>
    ),
}));

const FLOW_URN = 'urn:li:dataFlow:(airflow,test-flow,PROD)';
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,input,PROD)';

function renderControl(fetchStatus: FetchStatus, isExpanded: boolean) {
    const flow: LineageEntity = {
        id: FLOW_URN,
        urn: FLOW_URN,
        type: EntityType.DataFlow,
        isExpanded: { [LineageDirection.Upstream]: isExpanded, [LineageDirection.Downstream]: false },
        fetchStatus: { [LineageDirection.Upstream]: fetchStatus, [LineageDirection.Downstream]: fetchStatus },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
    const context = {
        nodes: new Map([[FLOW_URN, flow]]),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map([[FLOW_URN, new Set([DATASET_URN])]]),
            [LineageDirection.Downstream]: new Map(),
        },
    } as unknown as NodeContext;
    const props = {
        data: { urn: FLOW_URN, type: EntityType.DataFlow, direction: LineageDirection.Upstream },
    } as NodeProps<DataFlowLineageControlData>;

    return render(
        <MockedProvider>
            <TestPageContainer>
                <ReactFlowProvider>
                    <LineageNodesContext.Provider value={context}>
                        <DataFlowLineageControlNode {...props} />
                    </LineageNodesContext.Provider>
                </ReactFlowProvider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('DataFlowLineageControlNode', () => {
    it('shows no control while the flow lineage is still loading', () => {
        renderControl(FetchStatus.LOADING, true);

        expect(screen.queryByText(/contract|expand/)).not.toBeInTheDocument();
    });

    it('offers to contract expanded flow lineage', () => {
        renderControl(FetchStatus.COMPLETE, true);

        expect(screen.getByText(`contract ${LineageDirection.Upstream}`)).toBeInTheDocument();
    });

    it('offers to expand contracted flow lineage, with its neighbor count', () => {
        renderControl(FetchStatus.COMPLETE, false);

        expect(screen.getByText(`expand ${LineageDirection.Upstream} 1`)).toBeInTheDocument();
    });
});
