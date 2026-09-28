import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter, Route } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import StructuredPropsTable from '@app/govern/structuredProperties/StructuredPropsTable';
import themeV2 from '@conf/theme/themeV2';

import { EntityType, StructuredPropertyEntity } from '@types';

const { softDelete, hardDelete, toastError } = vi.hoisted(() => ({
    softDelete: vi.fn(),
    hardDelete: vi.fn(),
    toastError: vi.fn(),
}));

vi.mock('@src/graphql/mutations.generated', () => ({
    useBatchUpdateSoftDeletedMutation: () => [softDelete],
}));

vi.mock('@src/graphql/structuredProperties.generated', () => ({
    useDeleteStructuredPropertyMutation: () => [hardDelete],
}));

vi.mock('@src/app/context/useUserContext', () => ({
    useUserContext: () => ({ platformPrivileges: { manageStructuredProperties: true } }),
}));

vi.mock('@src/app/analytics', () => ({
    default: { event: vi.fn() },
    EventType: {
        DeleteStructuredPropertyEvent: 'DeleteStructuredPropertyEvent',
        ViewStructuredPropertyEvent: 'ViewStructuredPropertyEvent',
    },
}));

vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityName: () => 'Dataset',
        getDisplayName: () => 'user',
    }),
}));

// Only `toast` is stubbed; the table renders real components from this barrel.
vi.mock('@components', async () => {
    const actual = await vi.importActual<typeof import('@components')>('@components');
    return { ...actual, toast: { ...actual.toast, error: toastError } };
});

const testProperty = {
    urn: 'urn:li:structuredProperty:io.acryl.test.deleteMe',
    type: EntityType.StructuredProperty,
    definition: {
        qualifiedName: 'io.acryl.test.deleteMe',
        displayName: 'deleteMe',
        valueType: { urn: 'urn:li:dataType:datahub.string' },
        entityTypes: [{ urn: 'urn:li:entityType:datahub.dataset', info: { type: EntityType.Dataset } }],
    },
} as unknown as StructuredPropertyEntity;

function renderTable() {
    render(
        <MemoryRouter>
            <ThemeProvider theme={themeV2}>
                <StructuredPropsTable
                    searchQuery="deleteMe"
                    loading={false}
                    fetchData={vi.fn().mockResolvedValue([])}
                    pageSize={10}
                    searchResults={[testProperty]}
                />
                <Route path="/structured-properties/edit/:urn">
                    <div data-testid="structured-property-page" />
                </Route>
            </ThemeProvider>
        </MemoryRouter>,
    );
}

async function confirmDeleteFromMenu() {
    fireEvent.click(screen.getByTestId('structured-props-more-options-icon'));
    fireEvent.click(await screen.findByTestId('structured-prop-action-delete'));
    fireEvent.click(await screen.findByTestId('modal-confirm-button'));
}

describe('StructuredPropsTable delete flow', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('soft-deletes the property before hard-deleting it', async () => {
        softDelete.mockResolvedValue({ errors: undefined });
        hardDelete.mockResolvedValue({ errors: undefined });
        renderTable();

        await confirmDeleteFromMenu();

        await waitFor(() => expect(hardDelete).toHaveBeenCalledTimes(1));
        expect(softDelete).toHaveBeenCalledWith({
            variables: { input: { urns: [testProperty.urn], deleted: true } },
        });
        expect(hardDelete).toHaveBeenCalledWith({
            variables: { input: { urn: testProperty.urn } },
        });
        // Soft delete must complete before the destructive delete fires
        expect(softDelete.mock.invocationCallOrder[0]).toBeLessThan(hardDelete.mock.invocationCallOrder[0]);
    });

    it('does not hard-delete when the soft delete fails', async () => {
        softDelete.mockRejectedValue(new Error('soft delete failed'));
        renderTable();

        await confirmDeleteFromMenu();

        await waitFor(() => expect(toastError).toHaveBeenCalledWith(expect.anything(), { duration: 3 }));
        expect(hardDelete).not.toHaveBeenCalled();
    });

    it('opens the property page when the row is clicked', async () => {
        renderTable();

        fireEvent.click(screen.getByTestId(testProperty.urn));

        expect(await screen.findByTestId('structured-property-page')).toBeInTheDocument();
    });
});
