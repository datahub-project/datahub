import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MockedFunction, vi } from 'vitest';

import { FetchStatus, LineageNodesContext, getEdgeId } from '@app/lineageV3/common';
import useFetchChildDataJobs from '@app/lineageV3/initialize/useFetchChildDataJobs';

import { useGetDataFlowDagQuery } from '@graphql/dataFlow.generated';
import { EntityType, LineageDirection } from '@types';

vi.mock('@graphql/dataFlow.generated', async (importOriginal) => {
    const actual = await importOriginal<any>();
    return {
        ...actual,
        useGetDataFlowDagQuery: vi.fn(),
    };
});

const ROOT_URN = 'urn:li:dataFlow:(airflow,test-flow,PROD)';
const REPO_URN = 'urn:li:dataset:(urn:li:dataPlatform:git,acme/repo1,PROD)';
const CHILD_JOB_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,test-flow,PROD),job1)';
const UPSTREAM_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,upstream1,PROD)';

function makeContextValue() {
    return {
        rootUrn: ROOT_URN,
        rootType: EntityType.DataFlow,
        nodes: new Map(),
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map(),
            [LineageDirection.Downstream]: new Map(),
        },
        nodeVersion: 0,
        setNodeVersion: vi.fn(),
        dataVersion: 0,
        setDataVersion: vi.fn(),
        displayVersion: [0, []] as [number, string[]],
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
}

function mockQueryOnce(data: any) {
    (useGetDataFlowDagQuery as MockedFunction<typeof useGetDataFlowDagQuery>).mockImplementation(
        ({ onCompleted }: any) => {
            onCompleted?.(data);
            return { data, loading: false, error: undefined } as any;
        },
    );
}

function renderWithContext(contextValue: ReturnType<typeof makeContextValue>) {
    return renderHook(() => useFetchChildDataJobs(), {
        wrapper: ({ children }) => (
            <MockedProvider>
                <LineageNodesContext.Provider value={contextValue as any}>{children}</LineageNodesContext.Provider>
            </MockedProvider>
        ),
    });
}

describe('useFetchChildDataJobs', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it("adds an edge for the DataFlow's own direct upstream repository when there are no child jobs", () => {
        const contextValue = makeContextValue();
        mockQueryOnce({
            dataFlow: {
                upstream: {
                    relationships: [{ entity: { urn: REPO_URN, type: EntityType.Dataset } }],
                },
                childJobs: { total: 0, relationships: [] },
            },
        });

        renderWithContext(contextValue);

        const edgeId = getEdgeId(ROOT_URN, REPO_URN, LineageDirection.Upstream);
        expect(contextValue.edges.get(edgeId)).toEqual(expect.objectContaining({ isDisplayed: true }));
        expect(contextValue.adjacencyList[LineageDirection.Upstream].get(ROOT_URN)?.has(REPO_URN)).toBe(true);
        // Upstream-only: laid out to the flow's left, and no downstream expansion to offer.
        expect(contextValue.nodes.get(REPO_URN)).toEqual(
            expect.objectContaining({
                urn: REPO_URN,
                type: EntityType.Dataset,
                direction: LineageDirection.Upstream,
                fetchStatus: expect.objectContaining({ [LineageDirection.Downstream]: FetchStatus.UNNEEDED }),
            }),
        );
    });

    it('still adds child-job nodes and their own upstream edges when the DataFlow has no direct upstream', () => {
        const contextValue = makeContextValue();
        mockQueryOnce({
            dataFlow: {
                upstream: { relationships: [] },
                childJobs: {
                    total: 1,
                    relationships: [
                        {
                            entity: {
                                __typename: 'DataJob',
                                urn: CHILD_JOB_URN,
                                type: EntityType.DataJob,
                                upstream: {
                                    relationships: [
                                        { entity: { urn: UPSTREAM_DATASET_URN, type: EntityType.Dataset } },
                                    ],
                                },
                            },
                        },
                    ],
                },
            },
        });

        renderWithContext(contextValue);

        expect(contextValue.nodes.get(CHILD_JOB_URN)).toEqual(expect.objectContaining({ parentDataJob: ROOT_URN }));
        const edgeId = getEdgeId(CHILD_JOB_URN, UPSTREAM_DATASET_URN, LineageDirection.Upstream);
        expect(contextValue.edges.get(edgeId)).toEqual(expect.objectContaining({ isDisplayed: true }));
        expect(contextValue.adjacencyList[LineageDirection.Upstream].get(ROOT_URN)).toBeUndefined();
    });

    it('adds both the direct upstream repository and child-job upstream edges together', () => {
        const contextValue = makeContextValue();
        mockQueryOnce({
            dataFlow: {
                upstream: {
                    relationships: [{ entity: { urn: REPO_URN, type: EntityType.Dataset } }],
                },
                childJobs: {
                    total: 1,
                    relationships: [
                        {
                            entity: {
                                __typename: 'DataJob',
                                urn: CHILD_JOB_URN,
                                type: EntityType.DataJob,
                                upstream: {
                                    relationships: [
                                        { entity: { urn: UPSTREAM_DATASET_URN, type: EntityType.Dataset } },
                                    ],
                                },
                            },
                        },
                    ],
                },
            },
        });

        renderWithContext(contextValue);

        const directEdgeId = getEdgeId(ROOT_URN, REPO_URN, LineageDirection.Upstream);
        const childEdgeId = getEdgeId(CHILD_JOB_URN, UPSTREAM_DATASET_URN, LineageDirection.Upstream);
        expect(contextValue.edges.get(directEdgeId)).toEqual(expect.objectContaining({ isDisplayed: true }));
        expect(contextValue.edges.get(childEdgeId)).toEqual(expect.objectContaining({ isDisplayed: true }));
        expect(contextValue.adjacencyList[LineageDirection.Upstream].get(ROOT_URN)?.has(REPO_URN)).toBe(true);
        expect(contextValue.nodes.get(REPO_URN)).toEqual(
            expect.objectContaining({ urn: REPO_URN, type: EntityType.Dataset }),
        );
    });
});
