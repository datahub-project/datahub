import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React, { useContext } from 'react';
import { NodeProps, ReactFlowProvider } from 'reactflow';

import LineageTransformationNode from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import { FetchStatus, LineageDisplayContext, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, LineageDirection } from '@types';

vi.mock('@graphql/query.generated', () => ({
    useGetQueryQuery: () => ({ data: undefined }),
}));

vi.mock('../../queries/useRefetchLineage', () => ({
    default: () => vi.fn(),
}));

const mockGetIcon = vi.fn().mockReturnValue(<span data-testid="type-icon">type-icon</span>);

vi.mock('../../../useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getIcon: mockGetIcon,
        getEntityUrl: vi.fn().mockReturnValue('/entity/test'),
    }),
    useEntityRegistryV2: () => ({
        getIcon: mockGetIcon,
        getEntityUrl: vi.fn().mockReturnValue('/entity/test'),
    }),
}));

function emptyFilters(): LineageEntity['filters'] {
    return {
        [LineageDirection.Upstream]: { facetFilters: new Map() },
        [LineageDirection.Downstream]: { facetFilters: new Map() },
    };
}

function buildNodeData(args: {
    urn: string;
    type: EntityType;
    entity?: Pick<FetchedEntityV2, 'name' | 'icon'>;
}): LineageEntity {
    const entity: FetchedEntityV2 | undefined = args.entity
        ? {
              urn: args.urn,
              type: args.type,
              name: args.entity.name,
              icon: args.entity.icon,
          }
        : undefined;

    return {
        id: args.urn,
        urn: args.urn,
        type: args.type,
        entity,
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        filters: emptyFilters(),
    };
}

function buildNodeProps(nodeData: LineageEntity): NodeProps<LineageEntity> {
    return {
        id: nodeData.urn,
        type: 'lineage-transformation',
        data: nodeData,
        selected: false,
        dragging: false,
        zIndex: 1,
        isConnectable: false,
        xPos: 0,
        yPos: 0,
    };
}

function RenderWithLineageContexts({ nodeData }: { nodeData: LineageEntity }) {
    const defaultNodesContext = useContext(LineageNodesContext);
    const defaultDisplayContext = useContext(LineageDisplayContext);
    const defaultVisualizationContext = useContext(LineageVisualizationContext);

    return (
        <ReactFlowProvider>
            <LineageNodesContext.Provider
                value={{
                    ...defaultNodesContext,
                    rootUrn: 'urn:li:dataset:home',
                    rootType: EntityType.Dataset,
                    nodes: new Map([[nodeData.urn, nodeData]]),
                }}
            >
                <LineageDisplayContext.Provider
                    value={{
                        ...defaultDisplayContext,
                        shownUrns: [nodeData.urn],
                    }}
                >
                    <LineageVisualizationContext.Provider value={defaultVisualizationContext}>
                        <LineageTransformationNode {...buildNodeProps(nodeData)} />
                    </LineageVisualizationContext.Provider>
                </LineageDisplayContext.Provider>
            </LineageNodesContext.Provider>
        </ReactFlowProvider>
    );
}

function renderNode(args: { urn: string; type: EntityType; entity?: Pick<FetchedEntityV2, 'name' | 'icon'> }) {
    const nodeData = buildNodeData(args);

    return render(
        <MockedProvider>
            <TestPageContainer>
                <RenderWithLineageContexts nodeData={nodeData} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('LineageTransformationNode', () => {
    beforeEach(() => {
        mockGetIcon.mockClear();
    });

    it('shows a type icon fallback for hydrated DataJobs without a platform logo', () => {
        renderNode({
            urn: 'urn:li:dataJob:(urn:li:dataFlow:(kafkaconnect,flow,PROD),task)',
            type: EntityType.DataJob,
            entity: { name: 'task' },
        });

        expect(screen.getAllByTestId('type-icon').length).toBeGreaterThan(0);
        expect(mockGetIcon).toHaveBeenCalledWith(EntityType.DataJob, 18);
        expect(document.querySelector('.ant-skeleton-avatar')).toBeNull();
    });

    it('keeps the loading skeleton until the DataJob entity hydrates', () => {
        renderNode({
            urn: 'urn:li:dataJob:(urn:li:dataFlow:(kafkaconnect,flow,PROD),task)',
            type: EntityType.DataJob,
        });

        expect(document.querySelector('.ant-skeleton-avatar')).not.toBeNull();
        expect(screen.queryByTestId('type-icon')).toBeNull();
    });

    it('prefers the platform logo over the type icon when present', () => {
        renderNode({
            urn: 'urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),task)',
            type: EntityType.DataJob,
            entity: { name: 'task', icon: 'assets/platforms/airflowlogo.png' },
        });

        expect(screen.getByRole('img')).toHaveAttribute('src', 'assets/platforms/airflowlogo.png');
        // Node contents use the logo; popover LineageCard still requests a typeIcon fallback.
        expect(mockGetIcon).not.toHaveBeenCalledWith(EntityType.DataJob, 18);
        expect(document.querySelector('.ant-skeleton-avatar')).toBeNull();
    });
});
