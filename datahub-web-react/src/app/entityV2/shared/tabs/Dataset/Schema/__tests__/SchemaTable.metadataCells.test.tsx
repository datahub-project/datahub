/**
 * Renders the real SchemaTable (not a stub) to cover the user-visible half of two-phase
 * loading: description/tag/term cells show skeleton placeholders while fullMetadataLoading
 * is true, fill in with real values once it is false, and show an explicit "unavailable"
 * marker when the full-metadata query failed. Field-path cells are never placeholders.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
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
    },
] as any[];

const schemaMetadata = { name: 'cells_ds', fields } as any;

const renderTable = (props: { fullMetadataLoading?: boolean; fullMetadataError?: boolean }) =>
    render(
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
        </MockedProvider>,
    );

describe('SchemaTable metadata cells across the two loading phases', () => {
    it('shows skeletons in the metadata columns while full metadata is loading, but real field paths', async () => {
        renderTable({ fullMetadataLoading: true });

        await waitFor(() => expect(screen.getByTestId('schema-field-user_id')).toBeInTheDocument());
        expect(screen.getByTestId('schema-field-user_name')).toBeInTheDocument();
        // description + tags + terms per row, two rows.
        expect(screen.getAllByTestId('metadata-cell-skeleton').length).toBeGreaterThanOrEqual(6);
        expect(screen.queryByText('Primary identifier')).not.toBeInTheDocument();
        expect(screen.queryByText('pii')).not.toBeInTheDocument();
    });

    it('fills the metadata columns with real values once full metadata has loaded', async () => {
        const { rerender } = renderTable({ fullMetadataLoading: true });
        await waitFor(() => expect(screen.getAllByTestId('metadata-cell-skeleton').length).toBeGreaterThan(0));

        rerender(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SchemaTable
                        rows={fields}
                        schemaMetadata={schemaMetadata}
                        editableSchemaMetadata={null}
                        expandedDrawerFieldPath={null}
                        setExpandedDrawerFieldPath={() => {}}
                        fullMetadataLoading={false}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(screen.getByText('Primary identifier')).toBeInTheDocument());
        expect(screen.getByText('Display name')).toBeInTheDocument();
        expect(screen.getByText('pii')).toBeInTheDocument();
        expect(screen.queryAllByTestId('metadata-cell-skeleton')).toHaveLength(0);
    });

    it('marks metadata cells unavailable when the full-metadata query failed', async () => {
        renderTable({ fullMetadataLoading: false, fullMetadataError: true });

        await waitFor(() => expect(screen.getByTestId('schema-field-user_id')).toBeInTheDocument());
        expect(screen.getAllByTestId('metadata-unavailable').length).toBeGreaterThanOrEqual(6);
        expect(screen.queryAllByTestId('metadata-cell-skeleton')).toHaveLength(0);
        expect(screen.queryByText('Primary identifier')).not.toBeInTheDocument();
    });
});
