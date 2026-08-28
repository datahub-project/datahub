import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import StructuredPropsTable from '@app/govern/structuredProperties/StructuredPropsTable';
import themeV2 from '@conf/theme/themeV2';

import { EntityType, StructuredPropertyEntity } from '@types';

const { softDelete, hardDelete, showToastMessage } = vi.hoisted(() => ({
    softDelete: vi.fn(),
    hardDelete: vi.fn(),
    showToastMessage: vi.fn(),
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

vi.mock('@src/app/sharedV2/toastMessageUtils', () => ({
    showToastMessage,
    ToastType: { ERROR: 'error', SUCCESS: 'success', LOADING: 'loading' },
}));

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
        <ThemeProvider theme={themeV2}>
            <StructuredPropsTable
                searchQuery="deleteMe"
                loading={false}
                setIsDrawerOpen={vi.fn()}
                setIsViewDrawerOpen={vi.fn()}
                selectedProperty={testProperty}
                setSelectedProperty={vi.fn()}
                fetchData={vi.fn().mockResolvedValue([])}
                pageSize={10}
                searchResults={[testProperty]}
            />
        </ThemeProvider>,
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

        await waitFor(() => expect(showToastMessage).toHaveBeenCalledWith('error', expect.anything(), 3));
        expect(hardDelete).not.toHaveBeenCalled();
    });
});
