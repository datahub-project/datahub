import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { EntityType, LineageDirection } from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { FetchStatus, LineageNodesContext } from '../../common';
import ManageLineageModal from '../ManageLineageModal';

// Mock the SearchSelect component to avoid rendering it
vi.mock('../../../entityV2/shared/components/styled/search/SearchSelect', () => ({
    SearchSelect: () => <div data-testid="mocked-search-select">Mocked SearchSelect</div>,
}));

// Mock the LineageEdges component
vi.mock('../LineageEdges', () => ({
    default: () => <div data-testid="mocked-lineage-edges">Mocked LineageEdges</div>,
}));

// Mock the useEntityRegistry hook
vi.mock('../../../useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: vi.fn().mockImplementation((type, entity) => {
            if (type === EntityType.DataPlatform) return 'Hive';
            return entity?.name || 'Test Entity';
        }),
        getEntityUrl: vi.fn().mockImplementation((urn) => `/entity/${urn}`),
        getSearchEntityTypes: vi.fn().mockReturnValue([EntityType.Dataset, EntityType.Dashboard]),
    }),
    useEntityRegistryV2: () => ({
        getDisplayName: vi.fn().mockImplementation((type, entity) => {
            if (type === EntityType.DataPlatform) return 'Hive';
            return entity?.name || 'Test Entity';
        }),
        getEntityUrl: vi.fn().mockImplementation((urn) => `/entity/${urn}`),
        getSearchEntityTypes: vi.fn().mockReturnValue([EntityType.Dataset, EntityType.Dashboard]),
    }),
}));

// Mock the useUserContext hook
vi.mock('../../../context/useUserContext', () => ({
    useUserContext: () => ({
        user: {
            urn: 'urn:li:corpuser:testUser',
            username: 'testUser',
            type: EntityType.CorpUser,
        },
    }),
}));

// Mock the useOnClickExpandLineage hook
const mockExpandOneLevel = vi.fn();
vi.mock('../../LineageEntityNode/useOnClickExpandLineage', () => ({
    useOnClickExpandLineage: () => mockExpandOneLevel,
}));

// Mock the updateLineageMutation
vi.mock('../../../../graphql/mutations.generated', () => ({
    useUpdateLineageMutation: () => [vi.fn().mockResolvedValue({ data: { updateLineage: true } })],
}));

describe('ManageLineageModal', () => {
    // Mock DataPlatform entity
    const mockPlatform = {
        urn: 'urn:li:dataPlatform:hive',
        type: EntityType.DataPlatform,
        name: 'hive',
        properties: {
            displayName: 'Hive',
            type: 'RELATIONAL_DB',
            datasetNameDelimiter: '.',
        },
    };

    // Mock LineageEntity
    const mockNode = {
        urn: 'urn:li:dataset:test',
        type: EntityType.Dataset,
        entity: {
            urn: 'urn:li:dataset:test',
            type: EntityType.Dataset,
            name: 'Test Dataset',
            platform: mockPlatform,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: {},
            [LineageDirection.Downstream]: {},
        },
        id: 'test-id',
        isExpanded: true,
    };

    const mockCloseModal = vi.fn();
    const mockRefetch = vi.fn();

    // Mock LineageNodesContext
    const mockLineageNodesContext = {
        rootUrn: 'urn:li:dataset:test',
        rootType: EntityType.Dataset,
        nodes: new Map([['urn:li:dataset:test', mockNode]]),
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map([['urn:li:dataset:test', new Set(['urn:li:dataset:upstream1'])]]),
            [LineageDirection.Downstream]: new Map([['urn:li:dataset:test', new Set(['urn:li:dataset:downstream1'])]]),
        },
        nodeVersion: 0,
        setNodeVersion: vi.fn(),
        dataVersion: 0,
        setDataVersion: vi.fn(),
        displayVersion: [0, []],
        setDisplayVersion: vi.fn(),
        columnEdgeVersion: 0,
        setColumnEdgeVersion: vi.fn(),
        hideTransformations: false,
        setHideTransformations: vi.fn(),
        showDataProcessInstances: false,
        setShowDataProcessInstances: vi.fn(),
        showGhostEntities: false,
        setShowGhostEntities: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders the modal with the correct title for upstream direction', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <LineageNodesContext.Provider value={mockLineageNodesContext as any}>
                        <ManageLineageModal
                            node={mockNode as any}
                            direction={LineageDirection.Upstream}
                            closeModal={mockCloseModal}
                            refetch={mockRefetch}
                        />
                    </LineageNodesContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Select the Upstreams to add to Test Dataset')).toBeInTheDocument();
        expect(screen.getByText('Search and Add')).toBeInTheDocument();
        expect(screen.getByText('Current Upstreams')).toBeInTheDocument();
        expect(screen.getByTestId('mocked-search-select')).toBeInTheDocument();
        expect(screen.getByTestId('mocked-lineage-edges')).toBeInTheDocument();
    });

    it('renders the modal with the correct title for downstream direction', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <LineageNodesContext.Provider value={mockLineageNodesContext as any}>
                        <ManageLineageModal
                            node={mockNode as any}
                            direction={LineageDirection.Downstream}
                            closeModal={mockCloseModal}
                            refetch={mockRefetch}
                        />
                    </LineageNodesContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Select the Downstreams to add to Test Dataset')).toBeInTheDocument();
        expect(screen.getByText('Search and Add')).toBeInTheDocument();
        expect(screen.getByText('Current Downstreams')).toBeInTheDocument();
    });

    it('calls closeModal when cancel button is clicked', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <LineageNodesContext.Provider value={mockLineageNodesContext as any}>
                        <ManageLineageModal
                            node={mockNode as any}
                            direction={LineageDirection.Upstream}
                            closeModal={mockCloseModal}
                            refetch={mockRefetch}
                        />
                    </LineageNodesContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

        const cancelButton = screen.getByText('Cancel');
        fireEvent.click(cancelButton);

        expect(mockCloseModal).toHaveBeenCalledTimes(1);
    });

    it('expands lineage if fetchStatus is UNFETCHED', () => {
        const unfetchedNode = {
            ...mockNode,
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
                [LineageDirection.Downstream]: FetchStatus.COMPLETE,
            },
        };

        render(
            <MockedProvider>
                <TestPageContainer>
                    <LineageNodesContext.Provider value={mockLineageNodesContext as any}>
                        <ManageLineageModal
                            node={unfetchedNode as any}
                            direction={LineageDirection.Upstream}
                            closeModal={mockCloseModal}
                            refetch={mockRefetch}
                        />
                    </LineageNodesContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

        // The expandOneLevel function should be called for unfetched status
        expect(mockExpandOneLevel).toHaveBeenCalled();
    });

    it('does not expand lineage if fetchStatus is already COMPLETE', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <LineageNodesContext.Provider value={mockLineageNodesContext as any}>
                        <ManageLineageModal
                            node={mockNode as any}
                            direction={LineageDirection.Upstream}
                            closeModal={mockCloseModal}
                            refetch={mockRefetch}
                        />
                    </LineageNodesContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

        // The expandOneLevel function should not be called for complete status
        expect(mockExpandOneLevel).not.toHaveBeenCalled();
    });
});
