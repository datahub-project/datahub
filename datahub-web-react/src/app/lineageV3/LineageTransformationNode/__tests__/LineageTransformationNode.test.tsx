import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React, { useContext, useState } from 'react';
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

// jsdom can't simulate the hover card's hover/focus behaviour, so the hover card Popover (the only one passing
// onOpenChange) gets a toggle that stands in for the cursor entering and leaving it.
vi.mock('@components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@components')>();
    const MockPopover = ({
        open,
        onOpenChange,
        content,
        children,
    }: {
        open?: boolean;
        onOpenChange?: (open: boolean) => void;
        content?: React.ReactNode;
        children?: React.ReactNode;
    }) => {
        if (!onOpenChange) return <actual.Popover content={content}>{children}</actual.Popover>;
        return (
            <>
                {children}
                <button type="button" onClick={() => onOpenChange(!open)}>
                    toggle hover
                </button>
                {open && <div data-testid="hover-card">{content}</div>}
            </>
        );
    };
    return { ...actual, Popover: MockPopover };
});

vi.mock('../../manualLineage/ManageLineageModal', () => ({
    default: ({ direction, closeModal }: { direction: LineageDirection; closeModal: () => void }) => (
        <div data-testid="manage-lineage-modal">
            {direction}
            <button type="button" onClick={closeModal}>
                close modal
            </button>
        </div>
    ),
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
    entity?: Pick<FetchedEntityV2, 'name' | 'icon' | 'canEditLineage' | 'exists'>;
}): LineageEntity {
    const entity: FetchedEntityV2 | undefined = args.entity
        ? {
              urn: args.urn,
              type: args.type,
              name: args.entity.name,
              icon: args.entity.icon,
              canEditLineage: args.entity.canEditLineage,
              exists: args.entity.exists,
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
    const [displayedMenuNode, setDisplayedMenuNode] = useState<string | null>(null);

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
                        displayedMenuNode,
                        setDisplayedMenuNode,
                    }}
                >
                    {/* Stands in for clicking the graph, which closes any open node menu */}
                    <button type="button" onClick={() => setDisplayedMenuNode(null)}>
                        click graph
                    </button>
                    <LineageVisualizationContext.Provider value={defaultVisualizationContext}>
                        <LineageTransformationNode {...buildNodeProps(nodeData)} />
                    </LineageVisualizationContext.Provider>
                </LineageDisplayContext.Provider>
            </LineageNodesContext.Provider>
        </ReactFlowProvider>
    );
}

function renderNode(args: {
    urn: string;
    type: EntityType;
    entity?: Pick<FetchedEntityV2, 'name' | 'icon' | 'canEditLineage' | 'exists'>;
}) {
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

    describe('hover card menu', () => {
        const urn = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),task)';
        const openMenu = () => {
            renderNode({ urn, type: EntityType.DataJob, entity: { name: 'task', canEditLineage: true, exists: true } });
            fireEvent.click(screen.getByText('toggle hover'));
            fireEvent.click(screen.getByTestId(`manage-lineage-menu-${urn}`));
        };

        it('keeps the hover card open while its menu is open', () => {
            openMenu();

            fireEvent.click(screen.getByText('toggle hover'));

            expect(screen.getByTestId('hover-card')).toBeInTheDocument();
            expect(screen.getByTestId('edit-upstream-lineage')).toBeInTheDocument();
        });

        it('keeps the manage lineage modal open after the hover card closes', () => {
            openMenu();
            fireEvent.click(screen.getByTestId('edit-upstream-lineage'));
            fireEvent.click(screen.getByText('click graph'));
            fireEvent.click(screen.getByText('toggle hover'));

            expect(screen.queryByTestId('hover-card')).not.toBeInTheDocument();
            expect(screen.getByTestId('manage-lineage-modal')).toHaveTextContent(LineageDirection.Upstream);

            fireEvent.click(screen.getByText('close modal'));

            expect(screen.queryByTestId('manage-lineage-modal')).not.toBeInTheDocument();
        });
    });
});
