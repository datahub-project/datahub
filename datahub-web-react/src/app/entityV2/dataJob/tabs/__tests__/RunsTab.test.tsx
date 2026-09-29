import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { RunsTab } from '@app/entityV2/dataJob/tabs/RunsTab';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetExecutionRunsDocument } from '@graphql/runs.generated';
import { DataProcessRunStatus, EntityType } from '@types';

const TEST_DATA_FLOW_URN = 'urn:li:dataFlow:(azure-data-factory,test-factory.test-pipeline,DEV)';

const mockRunsResponse = {
    entity: {
        __typename: 'DataFlow',
        runs: {
            __typename: 'DataProcessInstanceResult',
            count: 2,
            start: 0,
            total: 2,
            runs: [
                {
                    __typename: 'DataProcessInstance',
                    urn: 'urn:li:dataProcessInstance:run-123',
                    type: EntityType.DataProcessInstance,
                    created: {
                        __typename: 'AuditStamp',
                        time: 1702300800000,
                        actor: null,
                    },
                    name: 'run-123',
                    state: [
                        {
                            __typename: 'DataProcessRunEvent',
                            status: DataProcessRunStatus.Complete,
                            attempt: 1,
                            result: {
                                __typename: 'DataProcessInstanceRunResult',
                                resultType: 'SUCCESS',
                                nativeResultType: 'success',
                            },
                            timestampMillis: 1702300800000,
                            durationMillis: 5000,
                        },
                    ],
                    inputs: {
                        __typename: 'EntityRelationshipsResult',
                        relationships: [],
                    },
                    outputs: {
                        __typename: 'EntityRelationshipsResult',
                        relationships: [],
                    },
                    parentTemplate: {
                        __typename: 'EntityRelationshipsResult',
                        relationships: [],
                    },
                    externalUrl: 'https://adf.azure.com/run/run-123',
                },
                {
                    __typename: 'DataProcessInstance',
                    urn: 'urn:li:dataProcessInstance:run-456',
                    type: EntityType.DataProcessInstance,
                    created: {
                        __typename: 'AuditStamp',
                        time: 1702214400000,
                        actor: null,
                    },
                    name: 'run-456',
                    state: [
                        {
                            __typename: 'DataProcessRunEvent',
                            status: DataProcessRunStatus.Complete,
                            attempt: 1,
                            result: {
                                __typename: 'DataProcessInstanceRunResult',
                                resultType: 'FAILURE',
                                nativeResultType: 'failed',
                            },
                            timestampMillis: 1702214400000,
                            durationMillis: 3000,
                        },
                    ],
                    inputs: {
                        __typename: 'EntityRelationshipsResult',
                        relationships: [],
                    },
                    outputs: {
                        __typename: 'EntityRelationshipsResult',
                        relationships: [],
                    },
                    parentTemplate: {
                        __typename: 'EntityRelationshipsResult',
                        relationships: [],
                    },
                    externalUrl: null,
                },
            ],
        },
    },
};

const mockEmptyRunsResponse = {
    entity: {
        __typename: 'DataFlow',
        runs: {
            __typename: 'DataProcessInstanceResult',
            count: 0,
            start: 0,
            total: 0,
            runs: [],
        },
    },
};

function createMocks(data: typeof mockRunsResponse | typeof mockEmptyRunsResponse): MockedResponse[] {
    return [
        {
            request: {
                query: GetExecutionRunsDocument,
                variables: {
                    urn: TEST_DATA_FLOW_URN,
                    start: 0,
                    count: 20,
                },
            },
            result: {
                data,
            },
        },
    ];
}

function renderRunsTab(mocks: MockedResponse[]) {
    return render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer initialEntries={[`/pipelines/${TEST_DATA_FLOW_URN}`]}>
                <EntityContext.Provider
                    value={{
                        urn: TEST_DATA_FLOW_URN,
                        entityType: EntityType.DataFlow,
                        entityData: null,
                        baseEntity: {},
                        updateEntity: vi.fn(),
                        routeToTab: vi.fn(),
                        refetch: vi.fn(),
                        lineage: undefined,
                        loading: false,
                        dataNotCombinedWithSiblings: null,
                    }}
                >
                    <RunsTab />
                </EntityContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('RunsTab', () => {
    it('renders loading state initially', () => {
        renderRunsTab(createMocks(mockRunsResponse));
        expect(screen.getByText('Fetching runs...')).toBeInTheDocument();
    });

    it('renders table headers after loading', async () => {
        renderRunsTab(createMocks(mockRunsResponse));

        // Wait for loading to complete and table to render
        await waitFor(
            () => {
                expect(screen.queryByText('Fetching runs...')).not.toBeInTheDocument();
            },
            { timeout: 3000 },
        );

        // Verify table headers are present
        expect(screen.getByText('Time')).toBeInTheDocument();
        expect(screen.getByText('Run ID')).toBeInTheDocument();
        expect(screen.getByText('Status')).toBeInTheDocument();
        expect(screen.getByText('Inputs')).toBeInTheDocument();
        expect(screen.getByText('Outputs')).toBeInTheDocument();
    });

    it('renders empty table when no runs', async () => {
        renderRunsTab(createMocks(mockEmptyRunsResponse));

        // Wait for loading to complete
        await waitFor(
            () => {
                expect(screen.queryByText('Fetching runs...')).not.toBeInTheDocument();
            },
            { timeout: 3000 },
        );

        // Table headers should still be visible
        expect(screen.getByText('Time')).toBeInTheDocument();
        expect(screen.getByText('Run ID')).toBeInTheDocument();

        // No run data should be present
        expect(screen.queryByText('run-123')).not.toBeInTheDocument();
    });
});
