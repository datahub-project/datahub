/**
 * Tests the two-phase schema loading hooks against real Apollo queries (MockedProvider),
 * complementing SchemaTab.twophase.test.tsx which mocks the hook away to test the tab:
 * - useGetEntityWithSchema: Phase 1 (structural) resolves first; Phase 2 (full metadata)
 *   fires only after Phase 1 delivers data and is merged into the same single copy;
 *   structuralOnly suppresses Phase 2 entirely; `loading` covers both phases unless the
 *   caller opts into structuralFirst; refetch re-runs only Phase 2 once full data exists.
 * - useGetColumnTabCount: undefined while loading, then the structural field count.
 */
import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import { useGetColumnTabCount } from '@app/entityV2/dataset/profile/useGetColumnTabCount';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';

import { GetDatasetSchemaDocument, GetDatasetSchemaStructuralDocument } from '@graphql/dataset.generated';

const TEST_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,two_phase_ds,PROD)';
const OTHER_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,other_ds,PROD)';

// Mutable so a test can simulate in-tab navigation to another dataset.
const mockEntity = { urn: TEST_URN };

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: mockEntity.urn, entityType: 'DATASET', entityData: null }),
}));

// Hide-siblings mode short-circuits the sibling-combination pass, keeping these tests
// focused on the two-phase sequencing rather than sibling merge behaviour.
vi.mock('@app/entityV2/shared/useIsSeparateSiblingsMode', () => ({
    useIsSeparateSiblingsMode: () => true,
}));

const structuralFields = [
    { __typename: 'SchemaField', fieldPath: 'user_id', type: 'NUMBER', nullable: false },
    { __typename: 'SchemaField', fieldPath: 'user_name', type: 'STRING', nullable: true },
];

const structuralDataset = {
    dataset: {
        __typename: 'Dataset',
        urn: TEST_URN,
        schemaMetadata: { __typename: 'SchemaMetadata', name: 'two_phase_ds', fields: structuralFields },
        siblings: null,
        siblingsSearch: null,
    },
};

const fullDataset = {
    dataset: {
        __typename: 'Dataset',
        urn: TEST_URN,
        schemaMetadata: {
            __typename: 'SchemaMetadata',
            name: 'two_phase_ds',
            fields: structuralFields.map((f) => ({ ...f, description: `${f.fieldPath} description` })),
        },
        editableSchemaMetadata: null,
        siblings: null,
        siblingsSearch: null,
    },
};

const structuralMock = {
    request: {
        query: GetDatasetSchemaStructuralDocument,
        variables: { urn: TEST_URN, skipSiblingsSearch: false },
    },
    result: { data: structuralDataset },
};

const fullMock = {
    request: {
        query: GetDatasetSchemaDocument,
        variables: { urn: TEST_URN },
    },
    result: { data: fullDataset },
};

// MockedProvider resolves mocks on the next tick, so without a delay Phase 2 completes before
// waitFor can observe the Phase 1 window that the sequencing tests assert on.
const slowFullMock = { ...fullMock, delay: 300 };

const wrapperWith =
    (mocks: MockedResponse[]) =>
    ({ children }: { children: React.ReactNode }) => (
        <MockedProvider mocks={mocks} addTypename={false}>
            {children}
        </MockedProvider>
    );

describe('useGetEntityWithSchema two-phase sequencing', () => {
    beforeEach(() => {
        mockEntity.urn = TEST_URN;
    });

    it('resolves the structural phase first, then loads full metadata', async () => {
        const { result } = renderHook(() => useGetEntityWithSchema(undefined, undefined, true), {
            wrapper: wrapperWith([structuralMock, slowFullMock]),
        });

        expect(result.current.loading).toBe(true);
        expect(result.current.structuralSchemaMetadata).toBeNull();

        // Phase 1: structural rows available, full metadata still in flight. structuralFirst
        // callers see loading=false here so they can render the rows already.
        await waitFor(() => expect(result.current.structuralSchemaMetadata).not.toBeNull());
        expect(result.current.loading).toBe(false);
        expect(result.current.fullMetadataLoading).toBe(true);
        expect(result.current.structuralSchemaMetadata?.fields?.map((f) => f.fieldPath)).toEqual([
            'user_id',
            'user_name',
        ]);

        // Phase 2: full metadata (descriptions) arrives afterwards.
        await waitFor(() => expect(result.current.fullMetadataLoading).toBe(false));
        await waitFor(() => expect(result.current.entityWithSchema?.schemaMetadata).toBeTruthy());
        expect(result.current.entityWithSchema?.schemaMetadata?.fields?.[1]?.description).toEqual(
            'user_name description',
        );
        expect(result.current.fullMetadataError).toBeUndefined();
        expect(result.current.structuralSchemaError).toBeUndefined();
        // Single copy: once full metadata is merged in, the structural view is released.
        expect(result.current.structuralSchemaMetadata).toBeNull();
        // The merged copy still carries every structural field, in order.
        expect(result.current.entityWithSchema?.schemaMetadata?.fields?.map((f) => f.fieldPath)).toEqual([
            'user_id',
            'user_name',
        ]);
    });

    it('default callers stay loading until full metadata has settled', async () => {
        // Consumers other than SchemaTab read entityWithSchema, which never exposes structural
        // data, so loading=false with only Phase 1 done would hand them the empty fallback.
        const { result } = renderHook(() => useGetEntityWithSchema(), {
            wrapper: wrapperWith([structuralMock, slowFullMock]),
        });

        await waitFor(() => expect(result.current.structuralSchemaMetadata).not.toBeNull());
        expect(result.current.loading).toBe(true);

        await waitFor(() => expect(result.current.loading).toBe(false));
        expect(result.current.entityWithSchema?.schemaMetadata?.fields).toHaveLength(2);
    });

    it('refetch after Phase 2 re-runs only the full query and keeps rows on screen', async () => {
        const refreshedFull = {
            request: fullMock.request,
            result: {
                data: {
                    dataset: {
                        ...fullDataset.dataset,
                        schemaMetadata: {
                            ...fullDataset.dataset.schemaMetadata,
                            fields: fullDataset.dataset.schemaMetadata.fields.map((f) => ({
                                ...f,
                                description: `${f.fieldPath} edited`,
                            })),
                        },
                    },
                },
            },
        };
        // No second structural mock: if refetch re-ran Phase 1 it would fail on a missing mock.
        const { result } = renderHook(() => useGetEntityWithSchema(undefined, undefined, true), {
            wrapper: wrapperWith([structuralMock, fullMock, refreshedFull]),
        });
        await waitFor(() => expect(result.current.entityWithSchema?.schemaMetadata?.fields).toHaveLength(2));

        await act(async () => {
            await result.current.refetch();
        });

        expect(result.current.entityWithSchema?.schemaMetadata?.fields?.[0]?.description).toEqual('user_id edited');
        // The reload never re-raised the metadata skeletons or dropped the rows.
        expect(result.current.fullMetadataLoading).toBe(false);
        expect(result.current.loading).toBe(false);
        expect(result.current.structuralSchemaError).toBeUndefined();
    });

    it('navigating to another dataset evicts the previous data and reloads in two phases', async () => {
        const otherStructural = {
            request: { query: structuralMock.request.query, variables: { urn: OTHER_URN, skipSiblingsSearch: false } },
            result: {
                data: {
                    dataset: {
                        ...structuralDataset.dataset,
                        urn: OTHER_URN,
                        schemaMetadata: {
                            __typename: 'SchemaMetadata',
                            name: 'other_ds',
                            fields: [
                                { __typename: 'SchemaField', fieldPath: 'other_col', type: 'STRING', nullable: true },
                            ],
                        },
                    },
                },
            },
            delay: 200,
        };
        const otherFull = {
            request: { query: fullMock.request.query, variables: { urn: OTHER_URN } },
            result: {
                data: {
                    dataset: {
                        ...fullDataset.dataset,
                        urn: OTHER_URN,
                        schemaMetadata: {
                            __typename: 'SchemaMetadata',
                            name: 'other_ds',
                            fields: [
                                {
                                    __typename: 'SchemaField',
                                    fieldPath: 'other_col',
                                    type: 'STRING',
                                    nullable: true,
                                    description: 'other description',
                                },
                            ],
                        },
                    },
                },
            },
            delay: 300,
        };
        const { result, rerender } = renderHook(() => useGetEntityWithSchema(undefined, undefined, true), {
            wrapper: wrapperWith([structuralMock, fullMock, otherStructural, otherFull]),
        });
        await waitFor(() => expect(result.current.entityWithSchema?.schemaMetadata?.fields).toHaveLength(2));

        // Navigate: the hook instance survives, only the urn changes.
        mockEntity.urn = OTHER_URN;
        rerender();

        // Nothing from the previous dataset leaks while the new one loads.
        expect(result.current.entityWithSchema?.schemaMetadata).toBeUndefined();
        expect(result.current.structuralSchemaMetadata).toBeNull();
        expect(result.current.fullMetadataLoading).toBe(false);

        // Phase 1 for the new dataset, then Phase 2.
        await waitFor(() =>
            expect(result.current.structuralSchemaMetadata?.fields?.[0]?.fieldPath).toEqual('other_col'),
        );
        expect(result.current.fullMetadataLoading).toBe(true);
        expect(result.current.entityWithSchema?.schemaMetadata).toBeUndefined();
        await waitFor(() =>
            expect(result.current.entityWithSchema?.schemaMetadata?.fields?.[0]?.description).toEqual(
                'other description',
            ),
        );
        expect(result.current.fullMetadataLoading).toBe(false);
    });

    it('structuralOnly never fires the full metadata query', async () => {
        // No fullMock supplied: if the full query fired, MockedProvider would surface a
        // missing-mock error through fullMetadataError.
        const { result } = renderHook(() => useGetEntityWithSchema(undefined, true), {
            wrapper: wrapperWith([structuralMock]),
        });

        await waitFor(() => expect(result.current.structuralSchemaMetadata).not.toBeNull());
        expect(result.current.fullMetadataLoading).toBe(false);
        expect(result.current.fullMetadataError).toBeUndefined();
        // Without the full query, consumers fall back to entity-context data (null here).
        expect(result.current.entityWithSchema).toBeNull();
    });

    it('skip=true loads nothing', async () => {
        const { result } = renderHook(() => useGetEntityWithSchema(true), {
            wrapper: wrapperWith([]),
        });
        await waitFor(() => expect(result.current.loading).toBe(false));
        expect(result.current.structuralSchemaMetadata).toBeNull();
        expect(result.current.entityWithSchema).toBeNull();
    });

    it('a full-metadata failure sets fullMetadataError and ends fullMetadataLoading', async () => {
        const fullErrorMock = { request: fullMock.request, error: new Error('metadata boom') };
        const { result } = renderHook(() => useGetEntityWithSchema(), {
            wrapper: wrapperWith([structuralMock, fullErrorMock]),
        });

        await waitFor(() => expect(result.current.structuralSchemaMetadata).not.toBeNull());
        await waitFor(() => expect(result.current.fullMetadataError).toBeTruthy());
        // Loading must end on failure so cells fall back to structural content instead of
        // rendering skeletons forever; the tab-level banner is the error indicator.
        expect(result.current.fullMetadataLoading).toBe(false);
        expect(result.current.structuralSchemaMetadata?.fields).toHaveLength(2);
    });

    it('retry after a structural failure reloads Phase 1 and then runs Phase 2', async () => {
        // Mocks are consumed in order: the first structural request fails, the refetch succeeds,
        // and the full query (never fired before) starts on its own once Phase 1 has data.
        const errorMock = { request: structuralMock.request, error: new Error('boom') };
        const { result } = renderHook(() => useGetEntityWithSchema(), {
            wrapper: wrapperWith([errorMock, structuralMock, fullMock]),
        });
        await waitFor(() => expect(result.current.structuralSchemaError).toBeTruthy());
        expect(result.current.structuralSchemaMetadata).toBeNull();

        await act(async () => {
            await result.current.refetch();
        });

        // refetch resolves once both phases have settled: the structural copy has already been
        // merged into the full one by now.
        await waitFor(() => expect(result.current.entityWithSchema?.schemaMetadata?.fields).toHaveLength(2));
        expect(result.current.structuralSchemaMetadata).toBeNull();
        expect(result.current.structuralSchemaError).toBeUndefined();
        expect(result.current.fullMetadataError).toBeUndefined();
        expect(result.current.loading).toBe(false);
    });

    it('retry after a full-metadata failure re-runs both phases in order and clears the error', async () => {
        const fullErrorMock = { request: fullMock.request, error: new Error('metadata boom') };
        const { result } = renderHook(() => useGetEntityWithSchema(), {
            wrapper: wrapperWith([structuralMock, fullErrorMock, structuralMock, fullMock]),
        });
        await waitFor(() => expect(result.current.fullMetadataError).toBeTruthy());

        await act(async () => {
            await result.current.refetch();
        });

        await waitFor(() => expect(result.current.entityWithSchema?.schemaMetadata?.fields).toHaveLength(2));
        expect(result.current.fullMetadataError).toBeUndefined();
        expect(result.current.fullMetadataLoading).toBe(false);
        expect(result.current.entityWithSchema?.schemaMetadata?.fields?.[1]?.description).toEqual(
            'user_name description',
        );
    });

    it('surfaces a structural query failure via structuralSchemaError', async () => {
        const errorMock = { request: structuralMock.request, error: new Error('boom') };
        const { result } = renderHook(() => useGetEntityWithSchema(), {
            wrapper: wrapperWith([errorMock]),
        });
        await waitFor(() => expect(result.current.structuralSchemaError).toBeTruthy());
        expect(result.current.structuralSchemaMetadata).toBeNull();
    });
});

describe('useGetColumnTabCount', () => {
    it('is undefined while loading, then reports the structural field count', async () => {
        const { result } = renderHook(() => useGetColumnTabCount(), {
            wrapper: wrapperWith([structuralMock]),
        });
        expect(result.current).toBeUndefined();
        await waitFor(() => expect(result.current).toEqual(2));
    });
});
