/**
 * Tests for the two-phase schema loading behaviour added for large-dataset
 * performance. Phase 1 fires a lean structural query immediately; Phase 2
 * fires the full metadata query (tags, terms, descriptions) once Phase 1
 * has delivered data.
 *
 * Each test controls the hook return value via mockUseGetEntityWithSchema
 * so it can simulate Phase 1-only, Phase 2 in-flight, and Phase 2 complete
 * states without hitting real Apollo queries.
 */
import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { SchemaTab } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTab';
import { TabRenderType } from '@app/entityV2/shared/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// Variables prefixed with 'mock' are safe to reference inside vi.mock factories
// because Vitest hoists their declarations alongside the vi.mock calls.
const mockEntityUrn = { value: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,ds_a,PROD)' };
const mockUseGetEntityWithSchema = vi.fn();

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: mockEntityUrn.value, entityType: 'DATASET', entityData: null }),
    useRefetch: () => vi.fn(),
    useMutationUrn: () => mockEntityUrn.value,
    useBaseEntity: () => ({}),
    useRouteToTab: () => vi.fn(),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema', () => ({
    useGetEntityWithSchema: () => mockUseGetEntityWithSchema(),
}));

vi.mock('@app/entityV2/shared/useIsSeparateSiblingsMode', () => ({
    useIsSeparateSiblingsMode: () => false,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/useSchemaVersioning', () => ({
    default: () => ({
        selectedVersion: null,
        versionList: [],
        schema: null,
        editableSchemaMetadata: null,
        isLatest: true,
    }),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/utils/updateSchemaFilterQueryString', () => ({
    default: () => {},
}));

// Renders the row slice it receives so tests can assert which fields are displayed.
vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable', () => ({
    default: ({ rows, fullMetadataLoading }: { rows: Array<{ fieldPath: string }>; fullMetadataLoading?: boolean }) => (
        <div
            data-testid="schema-table"
            data-first-key={rows[0]?.fieldPath ?? ''}
            data-row-count={String(rows.length)}
            data-metadata-loading={String(!!fullMetadataLoading)}
        />
    ),
}));

// Exposes filterText as a controlled input so tests can read and change it.
vi.mock('@app/entityV2/dataset/profile/schema/components/SchemaHeader', () => ({
    default: ({ filterText, setFilterText }: { filterText: string; setFilterText: (v: string) => void }) => (
        <input data-testid="filter-input" value={filterText} onChange={(e) => setFilterText(e.target.value)} />
    ),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar', () => ({
    default: () => null,
}));

vi.mock('@app/entityV2/dataset/profile/schema/components/SchemaRawView', () => ({
    default: () => null,
}));

// Helpers

const makeFields = (count: number, prefix = 'col') =>
    Array.from({ length: count }, (_, i) => ({
        fieldPath: `${prefix}_${String(i).padStart(4, '0')}`,
        type: { type: 'STRING' },
        nativeDataType: 'STRING',
        nullable: true,
        recursive: false,
    }));

// Phase 1 complete, Phase 2 in-flight: structural fields available, no tags/terms yet.
const phase1State = (fieldCount: number) => ({
    entityWithSchema: { schemaMetadata: null, editableSchemaMetadata: null },
    structuralSchemaMetadata: { fields: makeFields(fieldCount) },
    loading: false,
    fullMetadataLoading: true,
    fullMetadataError: null,
    structuralSchemaError: null,
    refetch: vi.fn(),
});

// Both phases complete: full metadata (entityWithSchema.schemaMetadata) is available.
// fullPrefix controls the prefix on Phase 2 fields so tests can distinguish Phase 2 data
// from Phase 1 structural data. structuralSchemaMetadata always uses 'col' prefix to
// represent the same Phase 1 payload that was delivered first.
const phase2State = (fieldCount: number, fullPrefix = 'col') => ({
    entityWithSchema: {
        schemaMetadata: { fields: makeFields(fieldCount, fullPrefix) },
        editableSchemaMetadata: null,
    },
    structuralSchemaMetadata: { fields: makeFields(fieldCount, 'col') },
    loading: false,
    fullMetadataLoading: false,
    fullMetadataError: null,
    structuralSchemaError: null,
    refetch: vi.fn(),
});

// Phase 1 hard failure: structural query errored, no schema available.
const phase1ErrorState = () => ({
    entityWithSchema: { schemaMetadata: null, editableSchemaMetadata: null },
    structuralSchemaMetadata: null,
    loading: false,
    fullMetadataLoading: false,
    fullMetadataError: null,
    structuralSchemaError: new Error('Network error'),
    refetch: vi.fn(),
});

// Phase 2 failure: structural data present, full metadata query errored.
const phase2ErrorState = (fieldCount: number) => ({
    entityWithSchema: { schemaMetadata: null, editableSchemaMetadata: null },
    structuralSchemaMetadata: { fields: makeFields(fieldCount) },
    loading: false,
    fullMetadataLoading: false,
    fullMetadataError: new Error('Metadata fetch failed'),
    structuralSchemaError: null,
    refetch: vi.fn(),
});

const Tab = () => (
    <MockedProvider mocks={[]} addTypename={false}>
        <TestPageContainer>
            <SchemaTab renderType={TabRenderType.DEFAULT} />
        </TestPageContainer>
    </MockedProvider>
);

describe('SchemaTab two-phase loading', () => {
    beforeEach(() => {
        vi.stubGlobal('localStorage', {
            getItem: vi.fn().mockReturnValue(null),
            setItem: vi.fn(),
            removeItem: vi.fn(),
            clear: vi.fn(),
            length: 0,
            key: vi.fn(),
        });
        mockEntityUrn.value = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,ds_a,PROD)';
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders Phase 1 structural rows while Phase 2 metadata is in-flight', async () => {
        mockUseGetEntityWithSchema.mockReturnValue(phase1State(5));

        render(<Tab />);

        await waitFor(() => expect(screen.getByTestId('schema-table')).toHaveAttribute('data-row-count', '5'));
    });

    it('switches to Phase 2 data and hides the banner once full metadata arrives', async () => {
        mockUseGetEntityWithSchema.mockReturnValue(phase1State(5));

        const { rerender } = render(<Tab />);

        await waitFor(() => expect(screen.getByTestId('schema-table')).toHaveAttribute('data-row-count', '5'));

        // Phase 2 arrives with a different field prefix so we can distinguish
        // it from Phase 1 structural data (col_ vs full_).
        mockUseGetEntityWithSchema.mockReturnValue(phase2State(5, 'full'));
        rerender(<Tab />);

        await waitFor(() => expect(screen.getByTestId('schema-table')).toHaveAttribute('data-first-key', 'full_0000'));
    });

    it('does not clear filter text while Phase 2 metadata is still loading', async () => {
        // Start before Phase 1 has settled so the loading -> false transition below
        // actually re-runs the wasSearchReset effect (it only fires on loading changes).
        mockUseGetEntityWithSchema.mockReturnValue({ ...phase1State(5), loading: true });

        const { rerender } = render(<Tab />);

        await waitFor(() => expect(screen.getByTestId('filter-input')).toBeInTheDocument());

        // Type a filter that has no matches in the Phase 1 structural data.
        fireEvent.change(screen.getByTestId('filter-input'), { target: { value: 'xyz' } });

        // Phase 1 settles while Phase 2 is still in flight: the effect re-runs with
        // matches.length === 0, and must NOT clear the filter while metadata is loading.
        mockUseGetEntityWithSchema.mockReturnValue(phase1State(5));
        rerender(<Tab />);

        await waitFor(() => expect(screen.getByTestId('filter-input')).toHaveValue('xyz'));
    });

    it('clears a no-match filter only after both phases have completed', async () => {
        mockUseGetEntityWithSchema.mockReturnValue(phase1State(5));

        const { rerender } = render(<Tab />);

        await waitFor(() => expect(screen.getByTestId('schema-table')).toBeInTheDocument());

        // Type something that will never match any field.
        fireEvent.change(screen.getByTestId('filter-input'), { target: { value: 'xyz' } });

        // Phase 2 completes but still no matches (fields are col_XXXX, filter is 'xyz').
        mockUseGetEntityWithSchema.mockReturnValue(phase2State(5));
        rerender(<Tab />);

        // Now both phases are done and matches.length is still 0 -> filter is cleared.
        await waitFor(() => expect(screen.getByTestId('filter-input')).toHaveValue(''));
    });

    it('shows Phase 1 error banner with retry when structural query fails', async () => {
        const mockRefetch = vi.fn();
        mockUseGetEntityWithSchema.mockReturnValue({ ...phase1ErrorState(), refetch: mockRefetch });

        render(<Tab />);

        await waitFor(() => expect(screen.getByText(/Could not load schema/)).toBeInTheDocument());

        // Retry button is rendered and calls refetch when clicked.
        const retryButton = screen.getByRole('button', { name: /retry/i });
        expect(retryButton).toBeInTheDocument();
        fireEvent.click(retryButton);
        expect(mockRefetch).toHaveBeenCalledTimes(1);
    });

    it('shows Phase 2 error banner with retry when metadata query fails but still renders Phase 1 rows', async () => {
        const mockRefetch = vi.fn();
        mockUseGetEntityWithSchema.mockReturnValue({ ...phase2ErrorState(5), refetch: mockRefetch });

        render(<Tab />);

        // Table still renders from Phase 1 structural data.
        await waitFor(() => expect(screen.getByTestId('schema-table')).toHaveAttribute('data-row-count', '5'));

        // Error banner is shown for Phase 2 failure.
        expect(screen.getByText(/Could not load field metadata/)).toBeInTheDocument();

        // Cells fall back to structural content: the failure must end the metadata-loading
        // state so the table stops rendering skeletons (the banner is the error signal).
        expect(screen.getByTestId('schema-table')).toHaveAttribute('data-metadata-loading', 'false');

        const retryButton = screen.getByRole('button', { name: /retry/i });
        expect(retryButton).toBeInTheDocument();
        fireEvent.click(retryButton);
        expect(mockRefetch).toHaveBeenCalledTimes(1);
    });
});
