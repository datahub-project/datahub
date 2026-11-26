import { toast } from '@components';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { EnvironmentActions } from '@app/settingsV2/environments/EnvironmentActions';
import themes from '@conf/theme/themes';

import { CustomFabricType, EntityType } from '@types';

const { deleteCustomFabricType, undeprecate } = vi.hoisted(() => ({
    deleteCustomFabricType: vi.fn(),
    undeprecate: vi.fn(),
}));

vi.mock('@graphql/customFabricType.generated', () => ({
    useDeleteCustomFabricTypeMutation: () => [deleteCustomFabricType],
}));

vi.mock('@app/entityV2/shared/EntityDropdown/useUndeprecateResource', () => ({
    useUndeprecateResource: () => undeprecate,
}));

vi.mock('@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal', () => ({
    UpdateDeprecationModal: () => <div data-testid="deprecate-modal" />,
}));

vi.mock('@components/components/Menu', () => ({
    Menu: ({
        items,
        children,
    }: {
        items: Array<{ key: string; title: string; onClick: () => void }>;
        children: React.ReactNode;
    }) => (
        <div>
            {children}
            {items.map((item) => (
                <button type="button" key={item.key} onClick={item.onClick}>
                    {item.title}
                </button>
            ))}
        </div>
    ),
}));

vi.mock('@app/sharedV2/modals/ConfirmationModal', () => ({
    ConfirmationModal: ({
        isOpen,
        handleConfirm,
        modalTitle,
    }: {
        isOpen: boolean;
        handleConfirm: () => void;
        modalTitle?: string;
    }) =>
        isOpen ? (
            <div>
                <div>{modalTitle}</div>
                <button type="button" aria-label="Confirm" data-testid="modal-confirm-button" onClick={handleConfirm} />
            </div>
        ) : null,
}));

const environment = (deprecated = false): CustomFabricType => ({
    urn: 'urn:li:customFabricType:PERF',
    type: EntityType.CustomFabricType,
    id: 'PERF',
    properties: { displayName: 'Performance' },
    deprecation: deprecated ? { deprecated: true, note: 'Retired', actor: 'urn:li:corpuser:admin' } : null,
});

const renderActions = (deprecated = false) => {
    const onEdit = vi.fn();
    const refetch = vi.fn();
    render(
        <ThemeProvider theme={themes.themeV2}>
            <EnvironmentActions environment={environment(deprecated)} onEdit={onEdit} refetch={refetch} />
        </ThemeProvider>,
    );
    return { onEdit, refetch };
};

describe('EnvironmentActions', () => {
    beforeEach(() => {
        deleteCustomFabricType.mockReset();
        undeprecate.mockReset();
        deleteCustomFabricType.mockResolvedValue({});
        vi.spyOn(toast, 'success').mockImplementation(() => 'id');
        vi.spyOn(toast, 'error').mockImplementation(() => 'id');
        Object.assign(navigator, { clipboard: { writeText: vi.fn().mockResolvedValue(undefined) } });
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('edits, copies the urn, and opens deprecate', () => {
        const { onEdit } = renderActions();

        fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
        expect(onEdit).toHaveBeenCalledWith(expect.objectContaining({ urn: 'urn:li:customFabricType:PERF' }));

        fireEvent.click(screen.getByRole('button', { name: 'Copy Urn' }));
        expect(navigator.clipboard.writeText).toHaveBeenCalledWith('urn:li:customFabricType:PERF');

        fireEvent.click(screen.getByRole('button', { name: 'Deprecate' }));
        expect(screen.getByTestId('deprecate-modal')).toBeInTheDocument();
    });

    it('clears deprecation when the environment is already deprecated', () => {
        renderActions(true);

        expect(screen.queryByRole('button', { name: 'Deprecate' })).not.toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Un-deprecate' }));
        expect(undeprecate).toHaveBeenCalledTimes(1);
    });

    it('deletes after confirmation', async () => {
        const { refetch } = renderActions();

        fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
        expect(screen.getByText('Delete environment PERF?')).toBeInTheDocument();
        fireEvent.click(screen.getByTestId('modal-confirm-button'));

        await waitFor(() =>
            expect(deleteCustomFabricType).toHaveBeenCalledWith({
                variables: { urn: 'urn:li:customFabricType:PERF' },
            }),
        );
        await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Deleted environment PERF.'));
        expect(refetch).toHaveBeenCalled();
        expect(screen.queryByTestId('modal-confirm-button')).not.toBeInTheDocument();
    });

    it('shows the server error when delete is refused', async () => {
        deleteCustomFabricType.mockRejectedValue(new Error('still in use'));
        renderActions();

        fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
        fireEvent.click(screen.getByTestId('modal-confirm-button'));

        await waitFor(() => expect(toast.error).toHaveBeenCalledWith('Failed to delete environment: still in use'));
        expect(toast.success).not.toHaveBeenCalled();
        await waitFor(() => expect(screen.queryByTestId('modal-confirm-button')).not.toBeInTheDocument());
    });
});
