/**
 * Tests the two-phase schema loading hooks against real Apollo queries (MockedProvider),
 * complementing SchemaTab.twophase.test.tsx which mocks the hook away to test the tab:
 * - useGetEntityWithSchema: Phase 1 (structural) resolves first; Phase 2 (full metadata)
 *   fires only after Phase 1 delivers data; structuralOnly suppresses Phase 2 entirely.
 * - useGetColumnTabCount: undefined while loading, then the structural field count.
 */
import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import { useGetColumnTabCount } from '@app/entityV2/dataset/profile/useGetColumnTabCount';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';

import { GetDatasetSchemaDocument, GetDatasetSchemaStructuralDocument } from '@graphql/dataset.generated';

const TEST_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,two_phase_ds,PROD)';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: TEST_URN, entityType: 'DATASET', entityData: null }),
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

const wrapperWith =
    (mocks: any[]) =>
    ({ children }: { children: React.ReactNode }) => (
        <MockedProvider mocks={mocks} addTypename={false}>
            {children}
        </MockedProvider>
    );

describe('useGetEntityWithSchema two-phase sequencing', () => {
    it('resolves the structural phase first, then loads full metadata', async () => {
        const { result } = renderHook(() => useGetEntityWithSchema(), {
            wrapper: wrapperWith([structuralMock, fullMock]),
        });

        expect(result.current.loading).toBe(true);
        expect(result.current.structuralSchemaMetadata).toBeNull();

        // Phase 1: structural rows available, full metadata still in flight.
        await waitFor(() => expect(result.current.structuralSchemaMetadata).not.toBeNull());
        expect(result.current.loading).toBe(false);
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
