import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import {
    MarketplaceEntityContextProvider,
    useMarketplaceEntityContext,
} from '@app/marketplace/context/MarketplaceEntityContext';

const mockManageAllDataProducts = vi.fn();

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({
        platformPrivileges: { manageAllDataProducts: mockManageAllDataProducts() },
    }),
}));

vi.mock('@app/marketplace/CreateMarketplaceDataProductModal', () => ({
    default: ({ open }: { open: boolean }) => (open ? <div data-testid="create-modal" /> : null),
}));

function CreateButton() {
    const { canCreateDataProduct, openCreateModal } = useMarketplaceEntityContext();
    return (
        <button type="button" data-can-create={String(canCreateDataProduct)} onClick={openCreateModal}>
            create
        </button>
    );
}

function renderProvider() {
    render(
        <MemoryRouter>
            <MarketplaceEntityContextProvider>
                <CreateButton />
            </MarketplaceEntityContextProvider>
        </MemoryRouter>,
    );
    return screen.getByRole('button', { name: 'create' });
}

describe('create data product gate', () => {
    it('opens the create modal with Manage All Data Products', () => {
        mockManageAllDataProducts.mockReturnValue(true);
        const button = renderProvider();

        expect(button).toHaveAttribute('data-can-create', 'true');
        fireEvent.click(button);
        expect(screen.getByTestId('create-modal')).toBeInTheDocument();
    });

    it('keeps the create modal closed without Manage All Data Products', () => {
        mockManageAllDataProducts.mockReturnValue(false);
        const button = renderProvider();

        expect(button).toHaveAttribute('data-can-create', 'false');
        fireEvent.click(button);
        expect(screen.queryByTestId('create-modal')).not.toBeInTheDocument();
    });
});
