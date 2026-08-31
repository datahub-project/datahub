import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { EntityType, SubResourceType } from '@src/types.generated';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

// The popover resolves a replacement column's parent to name it. Standing in for that lookup keeps
// the assertions on what the popover renders rather than on a fragment-shaped mock payload.
vi.mock('@graphql/entity.generated', () => ({
    useGetEntitiesQuery: ({ variables, skip }: { variables?: { urns?: string[] }; skip?: boolean }) => {
        const urn = variables?.urns?.[0] ?? '';
        if (skip || !urn) return { data: undefined };
        const name = urn.startsWith('urn:li:glossaryTerm:') ? 'My Term' : 'events';
        const type = urn.startsWith('urn:li:glossaryTerm:') ? 'GLOSSARY_TERM' : 'DATASET';
        return { data: { entities: [{ urn, type, name, properties: { name } }] } };
    },
}));

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)';

describe('DeprecationPill', () => {
    const defaultProps = {
        urn: 'urn:li:dataset:123',
        subResource: null,
        subResourceType: SubResourceType.DatasetField,
        showUndeprecate: false,
        refetch: vi.fn(),
    };

    // A schema field replacement renders a router Link to its parent's columns.
    const renderPill = (deprecation: any) =>
        render(
            <MockedProvider>
                <CustomThemeProvider>
                    <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                        <MemoryRouter>
                            <DeprecationIcon {...defaultProps} deprecation={deprecation} />
                        </MemoryRouter>
                    </EntityRegistryContext.Provider>
                </CustomThemeProvider>
            </MockedProvider>,
        );

    const openPopover = () => fireEvent.mouseEnter(screen.getByText('Deprecated'));

    it('correctly converts v2 schema field replacement path', async () => {
        renderPill({
            note: 'Deprecating this field',
            decommissionTime: null,
            deprecated: true,
            replacement: {
                urn: 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.schema.table,PROD),[version=2.0].[key=True].parent.[type=struct].child.[type=string])',
                type: EntityType.SchemaField,
            },
        });

        openPopover();
        await waitFor(() => {
            expect(screen.getByText(/parent\.child/)).toBeInTheDocument();
        });
    });

    it('shows note and decommission time when both present', async () => {
        renderPill({
            note: 'This is deprecated',
            decommissionTime: 1735689600, // Jan 1, 2025
            deprecated: true,
            replacement: null,
        });

        openPopover();
        await waitFor(() => {
            expect(screen.getByText('This is deprecated')).toBeInTheDocument();
            expect(screen.getByText(/Scheduled to be decommissioned/)).toBeInTheDocument();
        });
    });

    it('shows "No additional details" when no details provided', async () => {
        renderPill({ note: '', decommissionTime: null, deprecated: true, replacement: null });

        openPopover();
        await waitFor(() => {
            expect(screen.getByText('No additional details')).toBeInTheDocument();
        });
    });

    // A replacement is a detail of its own: on its own it used to read "No additional details".
    it('shows a replacement that comes without a note or a decommission time', async () => {
        renderPill({
            note: '',
            decommissionTime: null,
            deprecated: true,
            replacement: { urn: `urn:li:schemaField:(${DATASET_URN},col_a)`, type: EntityType.SchemaField },
        });

        openPopover();
        await waitFor(() => {
            expect(screen.getByText('events.col_a')).toBeInTheDocument();
        });
        expect(screen.queryByText('No additional details')).not.toBeInTheDocument();
    });

    it('links a replacement column to the columns of its parent dataset', async () => {
        renderPill({
            note: '',
            decommissionTime: null,
            deprecated: true,
            replacement: { urn: `urn:li:schemaField:(${DATASET_URN},col_a)`, type: EntityType.SchemaField },
        });

        openPopover();
        const link = await screen.findByRole('link', { name: 'events.col_a' });
        expect(link.getAttribute('href')).toContain('/Columns?highlightedPath=col_a');
    });

    // Only the dataset column route is known, so a column on a glossary term is named but not linked.
    it('names a replacement column on a glossary term without linking it', async () => {
        renderPill({
            note: '',
            decommissionTime: null,
            deprecated: true,
            replacement: {
                urn: 'urn:li:schemaField:(urn:li:glossaryTerm:my_term,term_col_a)',
                type: EntityType.SchemaField,
            },
        });

        openPopover();
        await waitFor(() => {
            expect(screen.getByText('My Term.term_col_a')).toBeInTheDocument();
        });
        expect(screen.queryByRole('link', { name: 'My Term.term_col_a' })).not.toBeInTheDocument();
    });
});
