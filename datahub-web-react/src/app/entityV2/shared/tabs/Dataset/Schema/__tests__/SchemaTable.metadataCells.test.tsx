/**
 * Renders the real SchemaTable (not a stub) to cover the user-visible half of two-phase
 * loading: description/tag/term cells show skeleton placeholders while fullMetadataLoading
 * is true, fill in with real values once it is false, and show an explicit "unavailable"
 * marker when the full-metadata query failed. Field-path cells are never placeholders.
 */
import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
import { MetadataStatus } from '@app/entityV2/shared/tabs/Dataset/Schema/metadataStatus';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,cells_ds,PROD)';

// Keep the real module (contexts, useBaseEntity, ...) and pin only the entity identity.
vi.mock('@app/entity/shared/EntityContext', async (importOriginal) => ({
    ...(await importOriginal<any>()),
    useEntityData: () => ({ urn: DATASET_URN, entityType: 'DATASET', entityData: null }),
    useMutationUrn: () => DATASET_URN,
    useRefetch: () => vi.fn(),
}));

// Structured-property columns come from a search query; none here.
vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetTableColumnProperties', () => ({
    useGetTableColumnProperties: () => undefined,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer', () => ({
    default: () => null,
}));

// jsdom has no layout, so the virtualised body would compute a zero-height viewport and
// render no rows. Plain antd table components render every row.
vi.mock('virtualizedtableforantd4', () => ({
    useVT: () => [{}, () => {}, { current: null }],
}));

const fields = [
    {
        fieldPath: 'user_id',
        type: 'NUMBER',
        nativeDataType: 'INT64',
        nullable: false,
        recursive: false,
        description: 'Primary identifier',
        globalTags: {
            tags: [{ tag: { urn: 'urn:li:tag:pii', type: 'TAG', name: 'pii', properties: { name: 'pii' } } }],
        },
    },
    {
        fieldPath: 'user_name',
        type: 'STRING',
        nativeDataType: 'STRING',
        nullable: true,
        recursive: false,
        description: 'Display name',
        globalTags: null,
        glossaryTerms: {
            terms: [
                {
                    term: {
                        urn: 'urn:li:glossaryTerm:Confidential',
                        type: 'GLOSSARY_TERM',
                        name: 'Confidential',
                        hierarchicalName: 'Confidential',
                        properties: { name: 'Confidential' },
                    },
                },
            ],
        },
    },
] as any[];

// description + tags + terms columns, two rows.
const METADATA_CELLS = 3 * fields.length;

const schemaMetadata = { name: 'cells_ds', fields } as any;

type TableProps = { metadataStatus?: MetadataStatus };

// One tree for render and rerender, so prop changes to SchemaTable land in one place.
const buildTable = (props: TableProps) => (
    <MockedProvider mocks={[]} addTypename={false}>
        <TestPageContainer>
            <SchemaTable
                rows={fields}
                schemaMetadata={schemaMetadata}
                editableSchemaMetadata={null}
                expandedDrawerFieldPath={null}
                setExpandedDrawerFieldPath={() => {}}
                {...props}
            />
        </TestPageContainer>
    </MockedProvider>
);

const renderTable = (props: TableProps) => render(buildTable(props));

describe('SchemaTable metadata cells across the two loading phases', () => {
    it('shows skeletons in the metadata columns while full metadata is loading, but real field paths', async () => {
        renderTable({ metadataStatus: 'loading' });

        await waitFor(() => expect(screen.getByTestId('schema-field-user_id')).toBeInTheDocument());
        expect(screen.getByTestId('schema-field-user_name')).toBeInTheDocument();
        expect(screen.getAllByTestId('metadata-cell-skeleton')).toHaveLength(METADATA_CELLS);
        expect(screen.queryByText('Primary identifier')).not.toBeInTheDocument();
        expect(screen.queryByText('pii')).not.toBeInTheDocument();
        expect(screen.queryByText('Confidential')).not.toBeInTheDocument();
    });

    it('fills the metadata columns with real values once full metadata has loaded', async () => {
        const { rerender } = renderTable({ metadataStatus: 'loading' });
        await waitFor(() => expect(screen.getAllByTestId('metadata-cell-skeleton').length).toBeGreaterThan(0));

        rerender(buildTable({ metadataStatus: 'ready' }));

        await waitFor(() => expect(screen.getByText('Primary identifier')).toBeInTheDocument());
        expect(screen.getByText('Display name')).toBeInTheDocument();
        expect(screen.getByText('pii')).toBeInTheDocument();
        expect(screen.getByText('Confidential')).toBeInTheDocument();
        expect(screen.queryAllByTestId('metadata-cell-skeleton')).toHaveLength(0);
    });

    it('sorts by name in both directions and keeps the sort when Phase 2 lands', async () => {
        const rowOrder = () =>
            screen.getAllByTestId(/^schema-field-(user_id|user_name)$/).map((el) => el.getAttribute('data-testid'));

        const { rerender } = renderTable({ metadataStatus: 'loading' });
        await waitFor(() => expect(screen.getByTestId('schema-field-user_id')).toBeInTheDocument());
        expect(rowOrder()).toEqual(['schema-field-user_id', 'schema-field-user_name']);

        // Ascending by name (already the fixture order), then descending. antd labels the
        // sortable header cell with aria-label; with a fixed table height the header lives in
        // its own table, so look it up by that label rather than by role.
        const nameHeader = screen.getAllByLabelText('Name')[0];
        fireEvent.click(nameHeader);
        await waitFor(() => expect(rowOrder()).toEqual(['schema-field-user_id', 'schema-field-user_name']));
        fireEvent.click(nameHeader);
        await waitFor(() => expect(rowOrder()).toEqual(['schema-field-user_name', 'schema-field-user_id']));

        // Phase 2 lands: rows are rebuilt with metadata, the descending sort must survive.
        rerender(buildTable({ metadataStatus: 'ready' }));
        await waitFor(() => expect(screen.getByText('Display name')).toBeInTheDocument());
        expect(rowOrder()).toEqual(['schema-field-user_name', 'schema-field-user_id']);
    });

    it('marks metadata cells unavailable when the full-metadata query failed', async () => {
        renderTable({ metadataStatus: 'error' });

        await waitFor(() => expect(screen.getByTestId('schema-field-user_id')).toBeInTheDocument());
        expect(screen.getAllByTestId('metadata-unavailable')).toHaveLength(METADATA_CELLS);
        expect(screen.queryByText('Confidential')).not.toBeInTheDocument();
        expect(screen.queryAllByTestId('metadata-cell-skeleton')).toHaveLength(0);
        expect(screen.queryByText('Primary identifier')).not.toBeInTheDocument();
    });
});
