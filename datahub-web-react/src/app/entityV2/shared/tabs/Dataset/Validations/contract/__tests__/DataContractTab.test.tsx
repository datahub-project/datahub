import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { DataContractTab } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/DataContractTab';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockEntityData = vi.hoisted(() => ({ value: { privileges: { canEditDataContract: false } } }));

vi.mock('@app/entityV2/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: 'urn:li:dataset:test', entityData: mockEntityData.value }),
}));

vi.mock('@graphql/contract.generated', () => ({
    useGetDatasetContractQuery: () => ({ data: { dataset: { contract: null } }, refetch: vi.fn() }),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/DataContractBuilderModal', () => ({
    DataContractBuilderModal: () => <div data-testid="contract-builder-modal" />,
}));

function renderTab(canEditDataContract: boolean) {
    mockEntityData.value = { privileges: { canEditDataContract } };
    render(
        <CustomThemeProvider>
            <DataContractTab />
        </CustomThemeProvider>,
    );
}

describe('DataContractTab', () => {
    it('opens the contract builder when the user can edit the contract', async () => {
        renderTab(true);

        await userEvent.click(screen.getByTestId('create-contract-button'));

        expect(screen.getByTestId('contract-builder-modal')).toBeInTheDocument();
    });

    it('disables create when the user cannot edit the contract', async () => {
        renderTab(false);

        expect(screen.getByTestId('create-contract-button')).toBeDisabled();
        expect(screen.queryByTestId('contract-builder-modal')).not.toBeInTheDocument();
    });
});
